package org.dataprov.dp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
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
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.expressions.ConcatWs
import org.apache.spark.sql.catalyst.plans.logical.Deduplicate
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.A
import org.apache.spark.sql.catalyst.plans.logical.Distinct

case class LogicalPlanWithProvenance(spark: SparkSession)
    extends Rule[LogicalPlan] {
    // Check if a plan already has provenance propagated
    def hasProv(plan: LogicalPlan, provenanceColName: String): Boolean =
        // LogicalPlanIntegrity.canGetOutputAttrs(plan) && 
        plan.output.exists(_.name == provenanceColName)

    // Find and get the provenance attribute in a plan 
    def getProvAttr(plan: LogicalPlan, provenanceColName: String): Attribute =
        // if (LogicalPlanIntegrity.canGetOutputAttrs(plan)) 
        plan.output.find(_.name == provenanceColName).get
        //  else throw new IllegalArgumentException("Plan is not resolved")

    // Custom tag to mark that a join has been processed to avoid infinite loops
    val PROCESSED_TAG: TreeNodeTag[Boolean] =
        TreeNodeTag[Boolean]("provenance_processed")
    
    // Custom tag to mark that a distinct has been processed to avoid infinite loops
    val PROCESSED_TAG2: TreeNodeTag[Boolean] =
        TreeNodeTag[Boolean]("provenance_processed_distinct")

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
                val validProjectList = projectList.filter(expr => expr.references.subsetOf(child.outputSet)) 
                
                // We check if the child has the provenance column and if the project itself already has it
                // We also check if the project list already contains an expression creating the provenance column
                val childHasProv = hasProv(child, provenanceColName)
                val projectHasProv = hasProv(p, provenanceColName)
                
                // Check if the projectList already contains an expression that creates the provenance column
                val projectListHasProvExpr = projectList.exists {
                    case Alias(_, name) => name == provenanceColName
                    case attr: Attribute => attr.name == provenanceColName
                    case _ => false
                }

                // Only add the provenance column if the child has it, the project doesn't,
                // and the project list isn't already creating it (to avoid duplicates)
                if (childHasProv && !projectHasProv && !projectListHasProvExpr) {
                    val provAttr = getProvAttr(child, provenanceColName)
                    val newProjectList = projectList :+ provAttr
                    p.copy(projectList = newProjectList, child = child)
                }  

                if (childHasProv && !projectHasProv && !projectListHasProvExpr) {
                    val provAttr = getProvAttr(child, provenanceColName)
                    p.copy(projectList = validProjectList :+ provAttr, child = child)
                } else if (validProjectList.size != projectList.size) {
                    // If we cleaned up dead references but the provenance column is already present, 
                    // we need to update the project list to remove the dead references
                    p.copy(projectList = validProjectList, child = child)
                } else {
                    p
                }

            // We look for 'Join' nodes, which represent JOIN statements
            case j @ Join(left, right, joinType, condition, hint) =>
                val leftHasProv = hasProv(left, provenanceColName)
                val rightHasProv = hasProv(right, provenanceColName)

                // We use a custom tag to check if this join has already been processed 
                // to avoid infinite loops when we add a new Project node on top of the join 
                // to combine the provenance tags from both sides.
                val isProcessed = j.getTagValue(PROCESSED_TAG).contains(true)

                if (!isProcessed && condition.isDefined && (leftHasProv || rightHasProv)) {
                    val leftProvAttr = getProvAttr(left, provenanceColName)
                    val rightProvAttr = getProvAttr(right, provenanceColName)

                    // ----- STRING -----
                    // We need to cast the provenance attributes to string to be able to concatenate them,
                    // as they can be of different types (e.g., string for one side and array for the other)
                    val leftProvCast = Cast(leftProvAttr, StringType)
                    val rightProvCast = Cast(rightProvAttr, StringType) 

                    // Operator ⊗ represents the combination of provenance tags from both sides of the join.
                    val matchedTag = Concat(
                    Seq(Literal("("), leftProvCast, Literal(" ⊗ "), rightProvCast, Literal(")"))
                    )

                    // The logic for combining provenance tags in a join is as follows:
                    // - If the left tag is null, it means we have a Right Outer Join and we keep the right tag.
                    // - If the right tag is null, it means we have a Left Outer Join and we keep the left tag.
                    // - If both tags are present, it means we have a Match and we combine the two tags using the ⊗ operator.
                    val joinLogicExpr = If(
                        IsNull(leftProvCast),
                        rightProvCast, // If the left tag is null (Right Outer Join), we keep the right one
                        If(
                            IsNull(rightProvCast),
                            leftProvCast, // If the right tag is null (Left Outer Join), we keep the left one
                            matchedTag // Otherwise, we combine the two (Match found)
                        )
                    )
                    // ------------------

                    // We create an alias for the combined provenance expression to give it the correct column name in the output
                    val combinedTag = Alias(joinLogicExpr, provenanceColName)()

                    // We mark the join as processed to avoid infinite loops
                    j.setTagValue(PROCESSED_TAG, true)
                    
                    // We clean the output to ensure having a unique provenance tag
                    val cleanedOutput = j.output.filter(_.name != provenanceColName)

                    // Create a new join with the combined provenance tag in output
                    j.setTagValue(PROCESSED_TAG, true)
                    
                    // Wrap in a Project to materialize the combined provenance column
                    Project(cleanedOutput :+ combinedTag, j)
                } else {
                    j
                }

            // We look for 'Filter' nodes, which represent WHERE statements
            case f @ Filter(condition, child) =>
                val childHasProv = hasProv(child, provenanceColName)
                val filterHasProv = hasProv(f, provenanceColName)
                // If the child has the provenance column but the filter does not, we need to add it to the filter condition.
                if (childHasProv && !filterHasProv) {
                    val provAttr = getProvAttr(child, provenanceColName)
                    val newCondition = And(condition, IsNotNull(provAttr))
                    Filter(newCondition, child)
                } else {
                    f
                }

            // We look for 'Sort' nodes, which represent ORDER BY statements
            case s @ Sort(order, global, child, hint) =>
                val childHasProv = hasProv(child, provenanceColName)
                val sortHasProv = hasProv(s, provenanceColName)
                // If the child has the provenance column but the sort does not, we need to add it to the sort order.
                if (childHasProv && !sortHasProv) {
                    val provAttr = getProvAttr(child, provenanceColName)
                    val newOrder = order :+ SortOrder(provAttr, Ascending)
                    Sort(newOrder, global, child, hint)
                } else {
                    s
                }   
            // We look for 'Aggregate' nodes, which represent GROUP BY statements
            case a @ Aggregate(groupingExprs, aggregateExprs, child, hint) =>
                val childHasProv = hasProv(child, provenanceColName)
                val aggregateHasProv = hasProv(a, provenanceColName)
                // If the child has the provenance column but the aggregate does not
                // we need to add it to the aggregate expressions.
                if (childHasProv && !aggregateHasProv) {
                    val provAttr = getProvAttr(child, provenanceColName)
                    val newAggregateExprs = aggregateExprs :+ Alias(CollectSet(provAttr), provenanceColName)()
                    Aggregate(groupingExprs, newAggregateExprs, child, hint)
                } else {
                    a
                }

            // We look for 'Distinct' nodes, which represent DISTINCT statements
            case d @ Distinct(child) => 
                // We ensure the child is tagged 
                val childHasProv = hasProv(child, provenanceColName)
                val childAttr = getProvAttr(child, provenanceColName)
                val childCast = Cast(childAttr, StringType)
                if (childHasProv && !d.getTagValue(PROCESSED_TAG2).contains(true)) {

                    // The columns of grouping must be all the columns of the child except the provenance column.
                    val groupingCols = child.output.filter(_.name != provenanceColName)

                    // We use the same logic as aggregation to merge the tags(A ⊕ B)
                    val collectSet = AggregateExpression(CollectSet(childCast), Complete, isDistinct = false)
                    val combinedTag = Alias(
                            Concat(Seq(
                                Literal("{"),
                                ConcatWs(Seq(Literal(" ⊕ "), collectSet)),
                                Literal("}")
                            )),
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

            

            }
          }
        }
    }


