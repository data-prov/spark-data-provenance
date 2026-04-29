package org.dataprov.dp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.sql.catalyst.expressions.ConcatWs
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.StringType
import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._

case class LogicalPlanWithProvenance(spark: SparkSession)
    extends Rule[LogicalPlan] {

  def ensureProv(plan: LogicalPlan, provenanceColName: String): LogicalPlan = {
    if (hasProv(plan, provenanceColName)) {
      plan
    } else {
      val newTag = Alias(
        Concat(
          Seq(
            Literal(s"${plan.nodeName}_"),
            Cast(MonotonicallyIncreasingID(), StringType)
          )
        ),
        provenanceColName
      )()
      Project(plan.output :+ newTag, plan)
    }
  }

  // Check if a plan already has provenance propagated
  def hasProv(plan: LogicalPlan, provenanceColName: String): Boolean =
    plan.output.exists(_.name == provenanceColName)

  // Find the provenance attribute in a plan
  def getProvAttr(plan: LogicalPlan, provenanceColName: String): Attribute =
    plan.output.find(_.name == provenanceColName).get

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
          // CRITICAL: Catalyst runs rules repeatedly until the plan stops changing.
          // We must check if we already added our column to avoid an infinite loop!
          if (hasProv(p, provenanceColName)) {
            p // Return the node unchanged
          } else {
            // We ensure the child is tagged with provenance
            val taggedChild = ensureProv(child, provenanceColName)

            // Create a new literal string column named '_provenance_tag_select'
            // val provenanceCol = Alias(Concat(Seq(Cast(MonotonicallyIncreasingID(), StringType))), PROV_COL)()
            val tagExpr = getProvAttr(taggedChild, provenanceColName)

            // Return a new Project node with our column appended to the list
            // Project(projectList :+ provenanceCol, child)
            p.copy(projectList = projectList :+ tagExpr, child = taggedChild)
          }

        // We look for 'Join' nodes, which represent JOIN statements
        case j @ Join(left, right, joinType, condition, hint) =>
          // CRITICAL: Catalyst runs rules repeatedly until the plan stops changing.
          // We must check if we already added our column to avoid an infinite loop!
          // Do not use 'hasProv' here because the join itself does not have the provenance column,
          // it is added in the parent 'Project' node. We use a custom tag to mark that we already processed this join.
          if (j.getTagValue(PROCESSED_TAG).contains(true)) {
            j // Return the node unchanged
          } else {
            // We ensure both sides of the join are tagged with provenance
            val taggedLeft = ensureProv(left, provenanceColName)
            val taggedRight = ensureProv(right, provenanceColName)

            // We recover the provenance tags of the children
            val leftTag = getProvAttr(taggedLeft, provenanceColName)
            val rightTag = getProvAttr(taggedRight, provenanceColName)

            // We create a new tag by combining the tags of the children
            val matchedTag = Concat(
              Seq(Literal("("), leftTag, Literal(" ⊗ "), rightTag, Literal(")"))
            )

            val joinLogicExpr = If(
              IsNull(leftTag),
              rightTag, // If the left tag is null (Right Outer Join), we keep the right one
              If(
                IsNull(rightTag),
                leftTag, // If the right tag is null (Left Outer Join), we keep the left one
                matchedTag // Otherwise, we combine the two (Match found)
              )
            )
            val combinedTag = Alias(joinLogicExpr, provenanceColName)()

            // The join is changed with tagged children
            val newJoin = j.copy(left = taggedLeft, right = taggedRight)

            // We mark the join as processed to avoid infinite loops
            newJoin.setTagValue(PROCESSED_TAG, true)

            // We clean the output to ensure having a unique provenance tag
            val cleanedOutput =
              newJoin.output.filter(_.name != provenanceColName)

            // The result is a new 'Project' tagged
            Project(cleanedOutput :+ combinedTag, newJoin)
          }

        // We look for 'Aggregate' nodes, which represent Aggregate statements
        case a @ Aggregate(_, aggExprs, child, hint) =>
          if (hasProv(a, provenanceColName)) {
            a // Return the node unchanged
          } else {
            // We ensure the child is tagged
            val taggedChild = ensureProv(child, provenanceColName)
            val childTag = getProvAttr(taggedChild, provenanceColName)

            // We recover the aggregate expression
            val collectSet = AggregateExpression(
              CollectSet(childTag),
              Complete,
              isDistinct = false
            )

            // We create a new tag by combining the tags of the children
            val combinedTag = Alias(
              Concat(
                Seq(
                  Literal("{"),
                  ConcatWs(Seq(Literal(" ⊕ "), collectSet)),
                  Literal("}")
                )
              ),
              provenanceColName
            )()
            // The aggregation is modified with the new tag
            a.copy(
              child = taggedChild,
              aggregateExpressions = aggExprs :+ combinedTag
            )
          }

        // We look for 'Filter' nodes, which represent WHERE statements
        case f @ Filter(condition, child) =>
          if (hasProv(f, provenanceColName)) {
            f
          } else {
            f.copy(child = ensureProv(child, provenanceColName))
          }

        // We look for 'Sort' nodes, which represent ORDER BY statements
        case s @ Sort(order, global, child, hint) =>
          if (hasProv(s, provenanceColName)) {
            s
          } else {
            s.copy(child = ensureProv(child, provenanceColName))
          }

        // We look for 'Distinct' nodes, which represent Distinct statements
        // We treat Distinct as a special case of Aggregate with all columns as
        // grouping columns and the same tag logic as Aggregate
        case Distinct(child) =>
          // We ensure the child is tagged
          val taggedChild = ensureProv(child, provenanceColName)
          val childTag = getProvAttr(taggedChild, provenanceColName)

          // The columns of grouping must be all the columns of the child except the provenance column.
          val groupingCols =
            taggedChild.output.filter(_.name != provenanceColName)

          // We use the same logic as aggregation to merge the tags(A ⊕ B)
          val collectSet = AggregateExpression(
            CollectSet(childTag),
            Complete,
            isDistinct = false
          )
          val combinedTag = Alias(
            Concat(
              Seq(
                Literal("{"),
                ConcatWs(Seq(Literal(" ⊕ "), collectSet)),
                Literal("}")
              )
            ),
            provenanceColName
          )()

          // We replace the Distinct node with an Aggregate node with
          // the same grouping columns and the new tag as aggregate expression
          Aggregate(
            groupingExpressions = groupingCols,
            aggregateExpressions = groupingCols :+ combinedTag,
            child = taggedChild
          )

      }
    }
  }
}
