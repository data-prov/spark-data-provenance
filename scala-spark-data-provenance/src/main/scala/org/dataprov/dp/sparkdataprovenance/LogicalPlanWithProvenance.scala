package org.dataprov.dp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.sql.catalyst.expressions.ConcatWs
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import org.dataprov.dp.sparkdataprovenance.DataFrameProvenanceTransformations._
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.plans.logical.Distinct

case class LogicalPlanWithProvenance(spark: SparkSession)
    extends Rule[LogicalPlan] {
    // Check if a plan already has provenance propagated
    def hasProv(plan: LogicalPlan, provenanceColName: String): Boolean =
        plan.output.exists(_.name == provenanceColName)

    // Find and get the provenance attribute in a plan 
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
                val childHasProv = hasProv(child, provenanceColName)
                val projectHasProv = hasProv(p, provenanceColName)

                if (childHasProv && !projectHasProv) {
                    // If the child has the provenance column but the project does not, we need to add it to the project list.
                    val provAttr = getProvAttr(child, provenanceColName)
                    Project(projectList :+ provAttr, child)
                } else {
                    p
                }   

        //     // We look for 'Join' nodes, which represent JOIN statements
        //     case j @ Join(left, right, _, _, _) =>
        //         val leftHasProv = hasProv(left, provenanceColName)
        //         val rightHasProv = hasProv(right, provenanceColName)
        //         val isProcessed = j.getTagValue(PROCESSED_TAG).contains(true)

        //         if (!isProcessed && (leftHasProv || rightHasProv)) {
        //             val leftProvAttr = getProvAttr(left, provenanceColName)
        //             val rightProvAttr = getProvAttr(right, provenanceColName)
        //             // Operator ⊗ represents the combination of provenance tags from both sides of the join.
        //             val matchedTag = Concat(
        //             Seq(Literal("("), leftProvAttr, Literal(" ⊗ "), rightProvAttr, Literal(")"))
        //             )

        //             val joinLogicExpr = If(
        //                 IsNull(leftProvAttr),
        //                 rightProvAttr, // If the left tag is null (Right Outer Join), we keep the right one
        //                 If(
        //                     IsNull(rightProvAttr),
        //                     leftProvAttr, // If the right tag is null (Left Outer Join), we keep the left one
        //                     matchedTag // Otherwise, we combine the two (Match found)
        //                 )
        //             )
        //             val combinedTag = Alias(joinLogicExpr, provenanceColName)()

        //             // We mark the join as processed to avoid infinite loops
        //             j.setTagValue(PROCESSED_TAG, true)

        //             // We clean the output to ensure having a unique provenance tag
        //             val cleanedOutput = j.output.filter(_.name != provenanceColName)

        //             // The result is a new 'Project' tagged
        //             Project(cleanedOutput :+ combinedTag, j)
        //         } else {
        //             j
        //         }

        //     // We look for 'Filter' nodes, which represent WHERE statements
        //     case f @ Filter(condition, child) =>
        //         val childHasProv = hasProv(child, provenanceColName)
        //         val filterHasProv = hasProv(f, provenanceColName)
        //         if (childHasProv && !filterHasProv) {
        //             // If the child has the provenance column but the filter does not, we need to add it to the filter condition.
        //             val provAttr = getProvAttr(child, provenanceColName)
        //             val newCondition = And(condition, IsNotNull(provAttr))
        //             Filter(newCondition, child)
        //         } else {
        //             f
        //         }

        //     // We look for 'Sort' nodes, which represent ORDER BY statements
        //     case s @ Sort(order, global, child, hint) =>
        //         val childHasProv = hasProv(child, provenanceColName)
        //         val sortHasProv = hasProv(s, provenanceColName)
        //         if (childHasProv && !sortHasProv) {
        //             // If the child has the provenance column but the sort does not, we need to add it to the sort order.
        //             val provAttr = getProvAttr(child, provenanceColName)
        //             val newOrder = order :+ SortOrder(provAttr, Ascending)
        //             Sort(newOrder, global, child, hint)
        //         } else {
        //             s
        //         }
            
        //     case a @ Aggregate(groupingExprs, aggregateExprs, child, _) =>
        //         val childHasProv = hasProv(child, provenanceColName)
        //         val aggregateHasProv = hasProv(a, provenanceColName)
        //         if (childHasProv && !aggregateHasProv) {
        //             // If the child has the provenance column but the aggregate does not
        //             // we need to add it to the aggregate expressions.
        //             val provAttr = getProvAttr(child, provenanceColName)
        //             val newAggregateExprs = aggregateExprs :+ Alias(CollectSet(provAttr), provenanceColName)()
        //             Aggregate(groupingExprs, newAggregateExprs, child)
        //         } else {
        //             a
        //         }
            
        //     case d @ Distinct(child) =>
        //         val childHasProv = hasProv(child, provenanceColName)
        //         val distinctHasProv = hasProv(d, provenanceColName)
        //         if (childHasProv && !distinctHasProv) {
        //             // If the child has the provenance column but the distinct does not
        //             // we need to add it to the distinct output.
        //             val provAttr = getProvAttr(child, provenanceColName)
        //             Project(child.output :+ provAttr, d)
        //         } else {
        //             d
        //         }
          
          }
        }
    }
}

