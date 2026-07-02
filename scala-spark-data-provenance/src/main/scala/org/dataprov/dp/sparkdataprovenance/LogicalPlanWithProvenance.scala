package org.dataprov.dp.sparkdataprovenance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.CurrentRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.UnaryMinus
import org.apache.spark.sql.catalyst.plans.Cross
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.Deduplicate
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlanIntegrity
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.dataprov.dp.sparkdataprovenance.ProvenanceApi._
import org.apache.spark.sql.catalyst.expressions.UnboundedPreceding
import org.apache.spark.sql.catalyst.expressions.UnboundedFollowing
import org.apache.spark.sql.catalyst.expressions.{WindowExpression, WindowSpecDefinition, SpecifiedWindowFrame, RowFrame}
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.catalyst.plans.logical.Intersect

case class LogicalPlanWithProvenance(
    spark: SparkSession,
    provenanceBuilder: ProvenanceBuilder = DisplayStringProvenanceBuilder
) extends Rule[LogicalPlan] {

  // Check if a plan already has provenance propagated
  def hasProv(plan: LogicalPlan, provenanceColName: String): Boolean =
    LogicalPlanIntegrity.canGetOutputAttrs(plan) && plan.output.exists(
      _.name == provenanceColName
    )

  // Find and get the provenance attribute in a plan
  def getProvAttr(plan: LogicalPlan, provenanceColName: String): Attribute =
    if (LogicalPlanIntegrity.canGetOutputAttrs(plan))
      plan.output.reverse.find(_.name == provenanceColName).get
    else throw new IllegalArgumentException("Plan is not resolved")

  // Custom tag to mark that a join has been processed to avoid infinite loops
  val PROCESSED_TAG: TreeNodeTag[Boolean] =
    TreeNodeTag[Boolean]("provenance_processed")

  // Helper function to extract the numeric value from a boundary expression
  private def boundaryValue(boundary: Expression): Option[Long] = boundary match {
    case UnboundedPreceding             => Some(Long.MinValue)
    case UnboundedFollowing             => Some(Long.MaxValue)
    case CurrentRow                     => Some(0L)
    case UnaryMinus(Literal(value: Byte, _), _)  => Some(-value.toLong)
    case UnaryMinus(Literal(value: Short, _), _) => Some(-value.toLong)
    case UnaryMinus(Literal(value: Int, _), _)   => Some(-value.toLong)
    case UnaryMinus(Literal(value: Long, _), _)  => Some(-value)
    case Literal(value: Byte, _)        => Some(value.toLong)
    case Literal(value: Short, _)       => Some(value.toLong)
    case Literal(value: Int, _)         => Some(value.toLong)
    case Literal(value: Long, _)        => Some(value)
    case _                              => None
  }

  // Helper function to find the widest specified window frame among a sequence of window expressions
  // It returns an Option[SpecifiedWindowFrame] that represents the widest frame found
  private def widestSpecifiedWindowFrame(windowExprs: Seq[NamedExpression]): Option[SpecifiedWindowFrame] = {
    // Collect all specified window frames from the window expressions
    val frames = windowExprs.flatMap(_.collect {
      case WindowExpression(_, WindowSpecDefinition(_, _, frame: SpecifiedWindowFrame)) => frame
    })
    // Find the widest frame by comparing the lower and upper boundaries
    frames.headOption.map { firstFrame =>
      val compatibleFrames = frames.filter(_.frameType == firstFrame.frameType)

      // Find the widest lower boundary among compatible frames
      val widestLower = compatibleFrames
        .flatMap(frame => boundaryValue(frame.lower).map(_ -> frame.lower))
        .minByOption(_._1)
        .map(_._2)
        .getOrElse(firstFrame.lower)

      // Find the widest upper boundary among compatible frames
      val widestUpper = compatibleFrames
        .flatMap(frame => boundaryValue(frame.upper).map(_ -> frame.upper))
        .maxByOption(_._1)
        .map(_._2)
        .getOrElse(firstFrame.upper)

      SpecifiedWindowFrame(firstFrame.frameType, widestLower, widestUpper)
    }
  }

  // Spark show() uses ToPrettyString, which can assert if it evaluates nulls
  // on expressions marked non-nullable. For UNION branches without provenance,
  // provide a neutral non-null provenance value per type.
  private def unionMissingProvenanceValue(dataType: DataType): Expression =
    dataType match {
      case StringType => Literal("")
      case BooleanType => Literal(false)
      case arrayType: ArrayType => Literal.create(Seq.empty, arrayType)
      case _ => Literal.create(null, dataType)
    }

  private def normalizeUnionChildWithProvenance(
      child: LogicalPlan,
      provenanceColName: String,
      targetProvType: DataType,
      hasChildrenWithoutProv: Boolean
  ): LogicalPlan = {
    val childProvAttr = getProvAttr(child, provenanceColName)
    val needsNormalization =
      hasChildrenWithoutProv || childProvAttr.dataType != targetProvType

    if (!needsNormalization) {
      child
    } else {
      val projectedOutput = child.output.map {
        case attr: Attribute if attr.name == provenanceColName =>
          val nullableAttr = attr.withNullability(true)
          // Always cast from a nullable attribute to preserve nullable metadata
          // after optimizer rewrites (important for Dataset.show / ToPrettyString).
          val normalizedExpr = Cast(nullableAttr, targetProvType)
          Alias(normalizedExpr, provenanceColName)()
        case attr: Attribute => attr
      }
      Project(projectedOutput, child)
    }
  }

  private def addUnionDefaultProvenance(
      child: LogicalPlan,
      provenanceColName: String,
      targetProvType: DataType
  ): LogicalPlan = {
    val defaultProvExpr = Alias(
      unionMissingProvenanceValue(targetProvType),
      provenanceColName
    )()
    Project(child.output :+ defaultProvExpr, child)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Get Spark provenance configurations
    val provenanceColName: String = provenanceColumnName(spark)

    if (!isProvenanceEnabled(spark)) {
      plan // If the feature is not enabled, return the plan unchanged
    } else {
      // transformUp traverses the tree from the bottom leaves to the top root
      plan.transformUp {

        // // We look for 'Project' nodes, which represent SELECT statements
        case p @ Project(projectList, child) =>
          // We check if the child has the provenance column and if the project itself already has it
          val childHasProv = hasProv(child, provenanceColName)

          // We partition the projectList into provenance expressions and non-provenance expressions
          val (provExprs, nonProvExprs) = projectList.partition {
            case Alias(_, name)  => name == provenanceColName
            case attr: Attribute => attr.name == provenanceColName
            case _               => false
          }

          // We filter the non-provenance expressions to keep only those that reference 
          // columns from the child output
          val validNonProvExprs = nonProvExprs.filter(expr =>
            expr.references.subsetOf(child.outputSet)
          )

          if (childHasProv) {
            val childProvAttr = getProvAttr(child, provenanceColName)
            // Reuse the existing provenance expression if all its references are still
            // satisfied by the current child output (multi-pass stability: a fresh alias
            // added on a previous pass is preserved unchanged on subsequent passes).
            // Otherwise create a fresh Alias so that two derivations of the same source
            // each get a distinct ExprId — required for correct self-join provenance
            // tracking (without this, both sides of the join share the same ExprId and
            // Spark resolves both references to the same row value).
            val provExpr = provExprs.collectFirst {
              case expr if expr.references.subsetOf(child.outputSet) => expr
            }.getOrElse(Alias(childProvAttr, provenanceColName)())
            p.copy(projectList = validNonProvExprs :+ provExpr, child = child)
          } 
          
          else if (validNonProvExprs.size != nonProvExprs.size) {
            // If some expressions were removed because they reference columns that are no longer present 
            // in the child output, we need to update the project list
            p.copy(projectList = validNonProvExprs, child = child)
          } else {
            p
          } 

        // We look for 'Filter' nodes, which represent WHERE statements
        case f @ Filter(_, _) =>
          // Filtering does not require provenance-specific rewrites.
          // Keep user predicate unchanged, including rows with null provenance.
          f

        // We look for 'Sort' nodes, which represent ORDER BY statements
        case s @ Sort(order, global, child, hint) =>
          // We check if the child has the provenance column and if the sort itself already has it
          val childHasProv = hasProv(child, provenanceColName)
          val sortHasProv = hasProv(s, provenanceColName)

          // If the child has the provenance column but the sort does not,
          // we need to add it to the sort order.
          if (childHasProv && !sortHasProv) {
            val provAttr = getProvAttr(child, provenanceColName)
            // We add the provenance column at the end of the sort order to ensure a deterministic
            // order of rows with the same values in the other sorted columns
            val newOrder = order :+ SortOrder(provAttr, Ascending)
            Sort(newOrder, global, child, hint)
          } else {
            s
          }

        // We look for 'Join' nodes, which represent JOIN statements
        case j @ Join(left, right, joinType, condition, _) =>
          // We check if the left and right children have the provenance column
          // and if the join itself already has it
          val leftHasProv = hasProv(left, provenanceColName)
          val rightHasProv = hasProv(right, provenanceColName)

          // We use a custom tag to check if this join has already been processed
          // to avoid infinite loops when we add a new Project node on top of the join
          // to combine the provenance tags from both sides.
          val isProcessed = j.getTagValue(PROCESSED_TAG).contains(true)

          if (
            !isProcessed && (condition.isDefined || joinType == Cross) && (leftHasProv || rightHasProv)
          ) {
            // We mark the join as processed to avoid infinite loops
            j.setTagValue(PROCESSED_TAG, true)

            // We clean the output to ensure having a unique provenance tag
            val cleanedOutput = j.output.filter(_.name != provenanceColName)

            if (leftHasProv && rightHasProv) {
              val leftProvAttr = getProvAttr(left, provenanceColName)
              val rightProvAttr = getProvAttr(right, provenanceColName)

              val joinLogicExpr = provenanceBuilder.join(
                leftProvAttr,
                rightProvAttr
              )

              // We create an alias for the combined provenance expression to give it
              // the correct column name in the output
              val combinedTag = Alias(joinLogicExpr, provenanceColName)()

              // Wrap in a Project to materialize the combined provenance column
              Project(cleanedOutput :+ combinedTag, j)

            } else if (leftHasProv) {
              val leftProvAttr = getProvAttr(left, provenanceColName)
              val combinedTag = Alias(
                provenanceBuilder.single(leftProvAttr),
                provenanceColName
              )()
              Project(cleanedOutput :+ combinedTag, j)

            } else {
              val rightProvAttr = getProvAttr(right, provenanceColName)
              val combinedTag = Alias(
                provenanceBuilder.single(rightProvAttr),
                provenanceColName
              )()
              Project(cleanedOutput :+ combinedTag, j)
            }
          } else {
            j
          }

        case i @ Intersect(left, right, isAll) =>
          // We check if the left and right children have the provenance column
          val leftHasProv = hasProv(left, provenanceColName)
          val rightHasProv = hasProv(right, provenanceColName)

          if (leftHasProv && rightHasProv) {
            val leftProvAttr = getProvAttr(left, provenanceColName)
            val rightProvAttr = getProvAttr(right, provenanceColName)

            val intersectLogicExpr = provenanceBuilder.join(
              leftProvAttr,
              rightProvAttr
            )

            val combinedTag = Alias(intersectLogicExpr, provenanceColName)()

            Project(i.output.filter(_.name != provenanceColName) :+ combinedTag, i)
          } else {
            i
          }

        // We look for 'Aggregate' nodes, which represent GROUP BY statements
        case a @ Aggregate(groupingExprs, aggregateExprs, child, hint) =>
          // We check if the child has the provenance column and if the aggregate itself already has it
          val childHasProv = hasProv(child, provenanceColName)
          val aggregateHasProv = hasProv(a, provenanceColName)

          // If the child has the provenance column but the aggregate itself does not,
          // add a provenance aggregation expression automatically.
          // If aggregate already has provenance (e.g., from DISTINCT rewrite), keep it as-is.
          if (childHasProv && !aggregateHasProv) {
            val provAttr = getProvAttr(child, provenanceColName)

            val newAggregateExprs = aggregateExprs :+ Alias(
              provenanceBuilder.aggregate(provAttr),
              provenanceColName
            )()

            Aggregate(groupingExprs, newAggregateExprs, child, hint)
          } else {
            a
          }

        case u @ Union(children, byName, allowMissingCol) =>
          // We check if any of the children have the provenance column
          val childrenWithProv = children.filter(hasProv(_, provenanceColName))
          childrenWithProv.headOption match {
            case None => u
            case Some(firstProvChild) =>
              val targetProvType = getProvAttr(firstProvChild, provenanceColName).dataType
              val hasChildrenWithoutProv = children.exists(
                child => !hasProv(child, provenanceColName)
              )

              val newChildren = children.map { child =>
                if (hasProv(child, provenanceColName)) {
                  normalizeUnionChildWithProvenance(
                    child,
                    provenanceColName,
                    targetProvType,
                    hasChildrenWithoutProv
                  )
                } else {
                  addUnionDefaultProvenance(
                    child,
                    provenanceColName,
                    targetProvType
                  )
                }
              }
              u.copy(children = newChildren)
          }

        // We look for 'Distinct' nodes, which represent DISTINCT statements without specified keys
        // (e.g., SELECT DISTINCT in SQL)
        case d @ Distinct(child) =>
          // We ensure the child is tagged
          val childHasProv = hasProv(child, provenanceColName)

          if (childHasProv) {

            val childAttr = getProvAttr(child, provenanceColName)

            // The columns of grouping must be all the columns of the child except the provenance column.
            val groupingCols = child.output.filter(_.name != provenanceColName)

            val combinedTag = Alias(
              provenanceBuilder.distinct(
                childAttr
              ),
              provenanceColName
            )()

            // We replace the Distinct node with an Aggregate node with
            // the same grouping columns and the new tag as aggregate expression
            Aggregate(
              groupingExpressions = groupingCols,
              aggregateExpressions = groupingCols :+ combinedTag,
              child = child
            )
          } else {
            d
          }

        // We look for 'Deduplicate' nodes, which represent Distinct statements with specified keys
        // (e.g., distinct or dropDuplicates in DataFrame API)
        case d @ Deduplicate(keys, child) =>
          // Keep only keys that are still available in the child output to avoid
          // analyzer failures when projection changed expression IDs upstream.
          val keysPresentInChild = keys.filter(k => child.outputSet.contains(k))

          // We ensure the child is tagged
          val childHasProv = hasProv(child, provenanceColName)
          
          if (childHasProv) {
            val provAttr = getProvAttr(child, provenanceColName)

            // The columns of grouping must be all the columns of the child except the provenance column.
            val validKeys =
              keysPresentInChild.filter(_.name != provenanceColName)

            val combinedTag = Alias(
              provenanceBuilder.distinct(
                provAttr
              ),
              provenanceColName
            )()

            // We replace the Deduplicate node with an Aggregate node with
            // the same grouping keys and the new tag as aggregate expression
            val newAggregateExprs = validKeys :+ combinedTag
            Aggregate(validKeys, newAggregateExprs, child)

          } else {
            // Even without provenance on the child, sanitize stale keys that are no
            // longer present after projection rewrites.
            if (keysPresentInChild.size != keys.size) {
              Deduplicate(keysPresentInChild, child)
            } else {
              d
            }
          }

        // We look for 'Window' nodes, which represent window functions (e.g., OVER clauses)
        case w @ Window(windowExprs, partitionSpec, orderSpec, child, _) =>
          val childHasProv = hasProv(child, provenanceColName)

          if (childHasProv) {
            val provAttr = getProvAttr(child, provenanceColName)
            val rawWindowColName = s"${provenanceColName}_raw_window"

            // We filter out any existing provenance expressions from the window expressions to avoid duplicates
            val userWindowExprs = windowExprs.filter {
              case Alias(_, name)  => name != provenanceColName && name != rawWindowColName
              case attr: Attribute => attr.name != provenanceColName && attr.name != rawWindowColName
              case _               => true
            }

            // We create a new window expression for the provenance column using the current child provenance attribute
            val baseAggExpr = provenanceBuilder.windowRaw(provAttr)
            val largestFrame = widestSpecifiedWindowFrame(userWindowExprs)
              .getOrElse(
                SpecifiedWindowFrame(
                  RowFrame,
                  UnboundedPreceding,
                  UnboundedFollowing
                )
              )
            // We create a new window specification for the provenance column using the same partition and 
            //order specifications as the user-defined window expressions
            val windowSpec = WindowSpecDefinition(
              partitionSpec,
              orderSpec,
              largestFrame
            )

            val windowProvExpr = WindowExpression(baseAggExpr, windowSpec)

            // Rebuild raw window provenance from the current child provenance
            // attribute to avoid stale exprIds in nested window rewrites.
            val rawWindowTag = Alias(windowProvExpr, provenanceColName)()

            val windowProv = w.copy(windowExpressions = userWindowExprs :+ rawWindowTag)

            Project(
              windowProv.output.filter(_.name != provenanceColName) :+ Alias(
                provenanceBuilder.windowFinalize(rawWindowTag.toAttribute),
                provenanceColName
              )(),
              windowProv
            )
          } else {
            w
          }
      }
    }
  }
}
