package org.dataprov.dp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
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
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._

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
      plan.output.find(_.name == provenanceColName).get
    else throw new IllegalArgumentException("Plan is not resolved")

  // Custom tag to mark that a join has been processed to avoid infinite loops
  val PROCESSED_TAG: TreeNodeTag[Boolean] =
    TreeNodeTag[Boolean]("provenance_processed")

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Get Spark provenance configurations
    val provenanceEnabled: Boolean = spark.sessionState.conf
      .getConfString(provenanceEnabledConf, "false") == "true"
    val provenanceColName: String = provenanceColumnName(spark)

    if (!provenanceEnabled) {
      plan // If the feature is not enabled, return the plan unchanged
    } else {
      // transformUp traverses the tree from the bottom leaves to the top root
      plan.transformUp {

        // We look for 'Project' nodes, which represent SELECT statements
        case p @ Project(projectList, child) =>
          // We clean the project list from any expression that references columns
          // not in the child output, as they would create dead references
          // and prevent the correct propagation of the provenance column
          val validProjectList = projectList.filter(expr =>
            expr.references.subsetOf(child.outputSet)
          )

          // We check if the child has the provenance column.
          // If yes, we force provenance to be the last projected column.
          val childHasProv = hasProv(child, provenanceColName)

          // Keep provenance expressions (if any) separate so we can place exactly one at the end.
          val (provExprs, nonProvExprs) = validProjectList.partition {
            case Alias(_, name)  => name == provenanceColName
            case attr: Attribute => attr.name == provenanceColName
            case _               => false
          }

          if (childHasProv) {
            val provExpr = provExprs.lastOption.getOrElse(getProvAttr(child, provenanceColName))
            val reorderedProjectList = nonProvExprs :+ provExpr

            if (reorderedProjectList != projectList) {
              p.copy(projectList = reorderedProjectList, child = child)
            } else {
              p
            }
          } else if (validProjectList.size != projectList.size) {
            // If we cleaned dead references, update the projection even without provenance.
            p.copy(projectList = validProjectList, child = child)
          } else {
            p
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

          if (!isProcessed && (condition.isDefined || joinType == Cross) && (leftHasProv || rightHasProv)) {
            // We mark the join as processed to avoid infinite loops
            j.setTagValue(PROCESSED_TAG, true)

            // We clean the output to ensure having a unique provenance tag
            val cleanedOutput = j.output.filter(_.name != provenanceColName)

            if(leftHasProv && rightHasProv) {
              val leftProvAttr = getProvAttr(left, provenanceColName)
              val rightProvAttr = getProvAttr(right, provenanceColName)
              
              val joinLogicExpr = provenanceBuilder.join(
                leftProvAttr,
                rightProvAttr
              )

              // We create an alias for the combined provenance expression to give it
              //  the correct column name in the output
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

        // We look for 'Filter' nodes, which represent WHERE statements
        case f @ Filter(condition, child) =>
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

        // We look for 'Aggregate' nodes, which represent GROUP BY statements
        // TODO: we may want to support a different operator for GROUP BY vs DISTINCT
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
            val validKeys = keysPresentInChild.filter(_.name != provenanceColName)

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

      }
    }
  }
}
