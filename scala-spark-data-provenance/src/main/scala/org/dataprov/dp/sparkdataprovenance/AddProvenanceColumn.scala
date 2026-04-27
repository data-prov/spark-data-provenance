package org.dataprov.dp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Join, Filter, Aggregate, LeafNode, Sort}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, MonotonicallyIncreasingID, Multiply, Concat, ConcatWs, Cast, Expression }
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Sum, AggregateExpression, CollectSet}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.plans.logical.Distinct

case class AddProvenanceColumn(spark: SparkSession) extends Rule[LogicalPlan] {


  def ensureProv(plan: LogicalPlan): LogicalPlan = {
    if (hasProv(plan)) {
      plan
    } else {
      val newTag = Alias(Concat(Seq(Literal(s"${plan.nodeName}_"), Cast(MonotonicallyIncreasingID(), StringType))), PROV_COL)()
      Project(plan.output :+ newTag, plan)
    }
  }

  // Check if a plan already has provenance propagated
  def hasProv(plan: LogicalPlan): Boolean = 
    plan.output.exists(_.name == PROV_COL)

  // Find the provenance attribute in a plan
  def getProvAttr(plan: LogicalPlan) = 
    plan.output.find(_.name == PROV_COL).get

  // Provenance tag
  val PROV_COL = "_provenance_tagged"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val isEnabled = spark.sessionState.conf.getConfString("spark.provenance.enabled", "false")
    if (isEnabled != "true"|| !plan.resolved) {
      return plan // If the feature is not enabled, return the plan unchanged
    }
    
    
    // transformUp traverses the tree from the bottom leaves to the top root
    plan.transformUp {
      
      // We look for 'Project' nodes, which represent SELECT statements
    
      case p @ Project(projectList, child) =>

        // CRITICAL: Catalyst runs rules repeatedly until the plan stops changing.
        // We must check if we already added our column to avoid an infinite loop!
        if (hasProv(p)) {
          p // Return the node unchanged
        } else {
          val taggedChild = ensureProv(child) 

          // Create a new literal string column named '_provenance_tag_select'
          // val provenanceCol = Alias(Concat(Seq(Cast(MonotonicallyIncreasingID(), StringType))), PROV_COL)()
          val tagExpr = getProvAttr(taggedChild)

          // Return a new Project node with our column appended to the list
          //Project(projectList :+ provenanceCol, child)
          p.copy(projectList = projectList :+ tagExpr, child = taggedChild)
        } 

      // We look for 'Join' nodes, which represent JOIN statements
      case j @ Join(left, right, joinType, condition, hint) =>
        if (hasProv(j)) {
          j // Return the node unchanged
        } else{
          // We search the provenance tag in each child
          val taggedLeft = ensureProv(left)
          val taggedRight = ensureProv(right)

          // We recover the provenance tag in each child
          val leftTag = getProvAttr(taggedLeft)
          val rightTag = getProvAttr(taggedRight)

          // We create a new tag by combining the tags of the children
          val combinedTag = Alias(Concat(Seq(Literal("("), leftTag, Literal(" ⊗ "), rightTag, Literal(")"))),PROV_COL)()
          // The join is changed with tagged children
          val newJoin = j.copy(left = taggedLeft, right = taggedRight)

          // We clean the output to ensure having a unique provenance tag
          val cleanedOutput = newJoin.output.filter(_.name != PROV_COL)
          // The result is a new 'Project' tagged 
          Project(cleanedOutput :+ combinedTag, newJoin)
          
        }
  
      // We look for 'Aggregate' nodes, which represent Aggregate statements
      case a @ Aggregate(groupingExprs, aggExprs, child, hint)  =>
        if (hasProv(a)) {
          a // Return the node unchanged
        } else {
          // We ensure the child is tagged and we recover the provenance tag
          val taggedChild = ensureProv(child)
          val childTag = getProvAttr(taggedChild)
          // We recover the aggregate expression
          val collectSet = AggregateExpression(CollectSet(childTag), Complete, isDistinct = false)
          // We create a new tag by combining the tags of the children
          val combinedTag = Alias(Concat(Seq(Literal("{"), ConcatWs(Seq(Literal(" ⊕ "), collectSet)), Literal("}"))), PROV_COL)()
          // The aggregation is modified with the new tag
          a.copy(child = taggedChild, aggregateExpressions = aggExprs :+ combinedTag)
        }

      case f @ Filter(condition, child) if (hasProv(child)) => f

      case s @ Sort(order, global, child, hint)  => s

      // We look for 'Distinct' nodes, which represent Distinct statements
      case d @ Distinct(child) => 
        // We ensure the child is tagged 
        val taggedChild = ensureProv(child)
        val childTag = getProvAttr(taggedChild)

        // The columns of grouping must be all the columns of the child except the provenance column.
        val groupingCols = taggedChild.output.filter(_.name != PROV_COL)

        // We use the same logic as aggregation to merge the tags(A ⊕ B)
        val collectSet = AggregateExpression(CollectSet(childTag), Complete, isDistinct = false)
        val combinedTag = Alias(Concat(Seq(Literal("{"), ConcatWs(Seq(Literal(" ⊕ "), collectSet)), Literal("}"))), PROV_COL)()
        
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
