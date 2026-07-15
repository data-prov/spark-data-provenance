package org.dataprov.dp.sparkdataprovenance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.ArrayDistinct
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.sql.catalyst.expressions.CreateArray
import org.apache.spark.sql.catalyst.expressions.CurrentRow
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.RowFrame
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame
import org.apache.spark.sql.catalyst.expressions.UnaryMinus
import org.apache.spark.sql.catalyst.expressions.UnboundedFollowing
import org.apache.spark.sql.catalyst.expressions.UnboundedPreceding
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.expressions.aggregate.Last
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.expressions.aggregate.MaxBy
import org.apache.spark.sql.catalyst.expressions.aggregate.Min
import org.apache.spark.sql.catalyst.expressions.aggregate.MinBy
import org.apache.spark.sql.catalyst.plans.Cross
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.Deduplicate
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.spark.sql.catalyst.plans.logical.Except
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Intersect
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.JoinHint
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlanIntegrity
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.dataprov.dp.sparkdataprovenance.ProvenanceApi._

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
  private def boundaryValue(boundary: Expression): Option[Long] =
    boundary match {
      case UnboundedPreceding                      => Some(Long.MinValue)
      case UnboundedFollowing                      => Some(Long.MaxValue)
      case CurrentRow                              => Some(0L)
      case UnaryMinus(Literal(value: Byte, _), _)  => Some(-value.toLong)
      case UnaryMinus(Literal(value: Short, _), _) => Some(-value.toLong)
      case UnaryMinus(Literal(value: Int, _), _)   => Some(-value.toLong)
      case UnaryMinus(Literal(value: Long, _), _)  => Some(-value)
      case Literal(value: Byte, _)                 => Some(value.toLong)
      case Literal(value: Short, _)                => Some(value.toLong)
      case Literal(value: Int, _)                  => Some(value.toLong)
      case Literal(value: Long, _)                 => Some(value)
      case _                                       => None
    }

  // Helper function to find the widest specified window frame among a sequence of window expressions
  // It returns an Option[SpecifiedWindowFrame] that represents the widest frame found
  private def widestSpecifiedWindowFrame(
      windowExprs: Seq[NamedExpression]
  ): Option[SpecifiedWindowFrame] = {
    // Collect all specified window frames from the window expressions
    val frames = windowExprs.flatMap(_.collect {
      case WindowExpression(
            _,
            WindowSpecDefinition(_, _, frame: SpecifiedWindowFrame)
          ) =>
        frame
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
      case StringType           => Literal("")
      case BooleanType          => Literal(false)
      case arrayType: ArrayType => Literal.create(Seq.empty, arrayType)
      case _                    => Literal.create(null, dataType)
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
            val provExpr = provExprs
              .collectFirst {
                case expr if expr.references.subsetOf(child.outputSet) => expr
              }
              .getOrElse(Alias(childProvAttr, provenanceColName)())
            p.copy(projectList = validNonProvExprs :+ provExpr, child = child)
          } else if (validNonProvExprs.size != nonProvExprs.size) {
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
        case j @ Join(left, right, joinType, condition, hint) =>
          // We check if the left and right children have the provenance column
          // and if the join itself already has it
          val leftHasProv = hasProv(left, provenanceColName)
          val rightHasProv = hasProv(right, provenanceColName)

          // We use a custom tag to check if this join has already been processed
          // to avoid infinite loops when we add a new Project node on top of the join
          // to combine the provenance tags from both sides.
          val isProcessed = j.getTagValue(PROCESSED_TAG).contains(true)

          if (
            !isProcessed && (condition.isDefined || joinType == Cross) &&
            (leftHasProv || rightHasProv) &&
            (joinType != LeftSemi && joinType != LeftAnti)
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

          } else if (!isProcessed && joinType == LeftSemi) {
            if (condition.isDefined) {

              if (rightHasProv) {
                val rightProvAttr = getProvAttr(right, provenanceColName)

                // We create a new physical row ID for the left side to ensure uniqueness
                val rowIdExpr = Alias(
                  MonotonicallyIncreasingID(),
                  s"${provenanceColName}_physical_row_id"
                )()
                val leftWithRowId = Project(left.output :+ rowIdExpr, left)

                // We create an Inner Join between the left and right children to get the matching rows
                val innerJoin =
                  Join(leftWithRowId, right, Inner, condition, hint)
                innerJoin.setTagValue(PROCESSED_TAG, true)

                // We elect a unique witness from the right by grouping by ALL the columns of the left.
                val rightWitnessExpr = Alias(
                  provenanceBuilder.distinct(rightProvAttr),
                  s"${provenanceColName}_right_witness"
                )()

                // We group by all the columns of the left side, including the physical row ID
                val groupingKeys = leftWithRowId.output
                val aggregatedLeft = Aggregate(
                  groupingKeys,
                  groupingKeys :+ rightWitnessExpr,
                  innerJoin
                )

                // The resulting left output should not include the provenance column,
                // as we will add a new combined tag
                val cleanedLeftOutput =
                  left.output.filter(_.name != provenanceColName)
                val rightWitnessAttr = rightWitnessExpr.toAttribute

                // We create a combined provenance tag based on whether the left side has provenance or not
                val combinedTag = if (leftHasProv) {
                  val leftProvAttr = getProvAttr(left, provenanceColName)
                  Alias(
                    provenanceBuilder.join(leftProvAttr, rightWitnessAttr),
                    provenanceColName
                  )()
                } else {
                  Alias(
                    provenanceBuilder.single(rightWitnessAttr),
                    provenanceColName
                  )()
                }
                // The left child becomes a clean Project containing the final combined tag
                val leftNew =
                  Project(cleanedLeftOutput :+ combinedTag, aggregatedLeft)

                // We reconstruct the original LeftSemi join at the TOP to satisfy Spark's Cast
                val topSemiJoin = j.copy(left = leftNew, right = right)
                topSemiJoin.setTagValue(PROCESSED_TAG, true)

                topSemiJoin
              } else {
                // If the right side does not have provenance, we can still propagate the left provenance
                val cleanedLeftOutput =
                  left.output.filter(_.name != provenanceColName)
                if (leftHasProv) {
                  val leftProvAttr = getProvAttr(left, provenanceColName)
                  val newTag = Alias(
                    provenanceBuilder.single(leftProvAttr),
                    provenanceColName
                  )()
                  val leftNew = Project(cleanedLeftOutput :+ newTag, left)

                  val topSemiJoin = j.copy(left = leftNew, right = right)
                  topSemiJoin.setTagValue(PROCESSED_TAG, true)
                  topSemiJoin
                } else {
                  j
                }
              }
            } else {
              j
            }
          } else {
            j
          }

        // We look for 'Intersect' nodes, which represent INTERSECT statements
        case i @ Intersect(left, right, isAll) if !isAll =>
          val leftHasProv = hasProv(left, provenanceColName)
          val rightHasProv = hasProv(right, provenanceColName)

          if (leftHasProv || rightHasProv) {
            // We need to rewrite the Intersect into an Inner Join to propagate provenance information
            val leftProvAttr = left.output.filter(_.name != provenanceColName)
            val rightProvAttr = right.output.filter(_.name != provenanceColName)

            // We create a join condition based on the equality of all non-provenance columns from both sides
            val joinCondition = leftProvAttr
              .zip(rightProvAttr)
              .map { case (l, r) =>
                EqualNullSafe(l, r)
              }
              .reduceLeftOption(And)

            // We create a new Join node with the join condition and the Inner join type
            joinCondition match {
              case Some(condition) =>
                // We create an Inner Join between the left and right children based on the join condition
                val innerJoin = Join(
                  left,
                  right,
                  Inner,
                  Some(condition),
                  hint = JoinHint.NONE
                )
                innerJoin.setTagValue(PROCESSED_TAG, true)

                // We create a combined provenance expression based on the presence of provenance in both sides
                val joinLogicExpr = (leftHasProv, rightHasProv) match {
                  case (true, true) =>
                    provenanceBuilder.join(
                      getProvAttr(left, provenanceColName),
                      getProvAttr(right, provenanceColName)
                    )
                  case (true, false) => getProvAttr(left, provenanceColName)
                  case (false, true) => getProvAttr(right, provenanceColName)
                  // Normally, this case should not happen because we check that at least one side has provenance, but we include it for completeness
                  case (false, false) =>
                    throw new IllegalStateException(
                      "At least one side must have provenance"
                    )
                }

                // We create an alias for the combined provenance expression to give it the correct column name in the output
                val combinedTag =
                  Alias(joinLogicExpr, s"${provenanceColName}_intersect_tmp")()

                // We create a Project node to include the combined provenance expression in the output of the Inner Join
                val projectWithCombinedProv =
                  Project(innerJoin.output :+ combinedTag, innerJoin)

                // We create an Aggregate node to group by all non-provenance columns and aggregate the combined provenance expression into a single provenance tag
                val groupingKeys = leftProvAttr
                val finalTag = Alias(
                  provenanceBuilder.aggregate(combinedTag.toAttribute),
                  provenanceColName
                )()

                Aggregate(
                  groupingKeys,
                  groupingKeys :+ finalTag,
                  projectWithCombinedProv
                )

              case None => i
            }
          } else {
            i
          }

        // We look for 'Aggregate' nodes, which represent GROUP BY statements
        case a @ Aggregate(groupingExprs, aggregateExprs, child, hint) =>
          val childHasProv = hasProv(child, provenanceColName)
          val aggregateHasProv = hasProv(a, provenanceColName)

          if (childHasProv && !aggregateHasProv) {
            val provAttr = getProvAttr(child, provenanceColName)

            // Extract all aggregate functions from the aggregate expressions
            val allAggFunctions = aggregateExprs.flatMap { expr =>
              expr.collect { case ae: AggregateExpression =>
                ae.aggregateFunction
              }
            }

            // Verify if there are any generic aggregate functions (SUM, AVG, etc.) in the list
            val hasGeneric = allAggFunctions.exists {
              case _: Max   => false
              case _: Min   => false
              case _: First => false
              case _: Last  => false
              case _        => true
            }

            val finalProvExpr = if (hasGeneric || allAggFunctions.isEmpty) {
              // CASE 1: Fallback if there are SUM, AVG, etc. -> global collect_list
              provenanceBuilder.aggregate(provAttr)
            } else {
              // CASE 2: Only MIN / MAX / FIRST / LAST (potentially multiple)
              val maxFunctions = allAggFunctions.collect { case max: Max =>
                max
              }
              val minFunctions = allAggFunctions.collect { case min: Min =>
                min
              }
              val firstFunctions = allAggFunctions.collect { case f: First =>
                f
              }
              val lastFunctions = allAggFunctions.collect { case l: Last => l }

              // Isolate unique target columns to avoid duplicate min_by
              val uniqueMaxChildren = maxFunctions.map(_.child).distinct
              val uniqueMinChildren = minFunctions.map(_.child).distinct

              // Generate a MaxBy witness for each target column of the MAX
              val maxByExprs = uniqueMaxChildren.map { child =>
                AggregateExpression(
                  MaxBy(provAttr, child),
                  Complete,
                  isDistinct = false
                )
              }

              // Generate a MinBy witness for each target column of the MIN
              val minByExprs = uniqueMinChildren.map { child =>
                AggregateExpression(
                  MinBy(provAttr, child),
                  Complete,
                  isDistinct = false
                )
              }

              // Generate a First witness for each target column of the FIRST
              val firstByExprs = firstFunctions.map { f =>
                val conditionedProv = If(
                  IsNull(f.child),
                  Literal.create(null, provAttr.dataType),
                  provAttr
                )
                AggregateExpression(
                  First(conditionedProv, f.ignoreNulls),
                  Complete,
                  isDistinct = false
                )
              }.distinct

              // Generate a Last witness for each target column of the LAST
              val lastByExprs = lastFunctions.map { l =>
                val conditionedProv = If(
                  IsNull(l.child),
                  Literal.create(null, provAttr.dataType),
                  provAttr
                )
                AggregateExpression(
                  Last(conditionedProv, l.ignoreNulls),
                  Complete,
                  isDistinct = false
                )
              }.distinct

              // Merge all witness generators
              val allWitnessExprs =
                maxByExprs ++ minByExprs ++ firstByExprs ++ lastByExprs

              val rawWitnessArray = if (allWitnessExprs.size == 1) {
                // If there is only one target column with a single MIN or MAX
                val singleExpr = allWitnessExprs.head
                provAttr.dataType match {
                  case StringType => CreateArray(Seq(singleExpr))
                  case _          => singleExpr
                }
              } else {
                // If there are multiple MIN, multiple MAX, or a mix etc.,
                // we need to combine them into a single array of witnesses
                provAttr.dataType match {
                  case StringType => ArrayDistinct(CreateArray(allWitnessExprs))
                  case _          => ArrayDistinct(Concat(allWitnessExprs))
                }
              }
              // We format the final provenance expression based on the data type of the provenance attribute
              provAttr.dataType match {
                case StringType =>
                  provenanceBuilder.formatWitnessArray(rawWitnessArray)
                case _ => rawWitnessArray
              }
            }

            // Injection of the final provenance tag
            val newAggregateExprs = aggregateExprs :+ Alias(
              finalProvExpr,
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
              val targetProvType =
                getProvAttr(firstProvChild, provenanceColName).dataType
              val hasChildrenWithoutProv =
                children.exists(child => !hasProv(child, provenanceColName))

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
        case d @ Distinct(child) =>
          // We ensure the child is tagged
          val childHasProv = hasProv(child, provenanceColName)

          if (childHasProv) {

            val childAttr = getProvAttr(child, provenanceColName)

            // The columns of grouping must be all the columns of the child except the provenance column.
            val groupingCols = child.output.filter(_.name != provenanceColName)

            val combinedTag = Alias(
              provenanceBuilder.distinct(childAttr),
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
              provenanceBuilder.distinct(provAttr),
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
              case Alias(_, name) =>
                name != provenanceColName && name != rawWindowColName
              case attr: Attribute =>
                attr.name != provenanceColName && attr.name != rawWindowColName
              case _ => true
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
            // order specifications as the user-defined window expressions
            val windowSpec = WindowSpecDefinition(
              partitionSpec,
              orderSpec,
              largestFrame
            )

            val windowProvExpr = WindowExpression(baseAggExpr, windowSpec)

            // Rebuild raw window provenance from the current child provenance
            // attribute to avoid stale exprIds in nested window rewrites.
            val rawWindowTag = Alias(windowProvExpr, provenanceColName)()

            val windowProv =
              w.copy(windowExpressions = userWindowExprs :+ rawWindowTag)

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

        // We look for 'Except' nodes, which represent EXCEPT statements
        case e @ Except(left, right, isAll) =>
          val leftHasProv = hasProv(left, provenanceColName)
          val cleanedOutput = e.output.filter(_.name != provenanceColName)
          if (leftHasProv) {
            val leftProvAttr = getProvAttr(left, provenanceColName)

            val exceptLogicExpr = provenanceBuilder.single(
              leftProvAttr
            )

            val combinedTag = Alias(exceptLogicExpr, provenanceColName)()

            Project(cleanedOutput :+ combinedTag, e)
          } else {
            e
          }
      }
    }
  }
}
