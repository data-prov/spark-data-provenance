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

  val PROV_COL = "_provenance_tag"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val isEnabled = spark.sessionState.conf.getConfString("spark.provenance.enabled", "false")
    if (isEnabled != "true"|| !plan.resolved) {
      return plan // If the feature is not enabled, return the plan unchanged
    }
    
    // Find the provenance attribute in a plan
    def getProvAttr(plan: LogicalPlan) = 
      plan.output.find(_.name == PROV_COL)

    // transformUp traverses the tree from the bottom leaves to the top root
    plan.transformUp {
      
      // We look for 'Project' nodes, which represent SELECT statements
    
      case p @ Project(projectList, child) =>

        // CRITICAL: Catalyst runs rules repeatedly until the plan stops changing.
        // We must check if we already added our column to avoid an infinite loop!
        if (hasProv(p)) {
          p // Return the node unchanged
        } else {
          val taggedChild = ensureProv(child) // Enveloppe l'enfant (ex: la View) s'il n'a pas de tag
          // Create a new literal string column named '_provenance_tag_select'
          // val provenanceCol = Alias(Concat(Seq(Cast(MonotonicallyIncreasingID(), StringType))), PROV_COL)()
          val tagExpr = getProvAttr(taggedChild).get
          // Return a new Project node with our column appended to the list
          //Project(projectList :+ provenanceCol, child)
          p.copy(projectList = projectList :+ tagExpr, child = taggedChild)
        } 

      // We look for 'Join' nodes, which represent JOIN statements
      case j @ Join(left, right, joinType, condition, hint) =>
        if (hasProv(j)) {
          j // Return the node unchanged
        } else{
          // We search the provenance tag in each table
          val taggedLeft = ensureProv(left)
          val taggedRight = ensureProv(right)

          val leftTag = getProvAttr(taggedLeft).get
          val rightTag = getProvAttr(taggedRight).get

          val combinedTag = Alias(Concat(Seq(Literal("("), leftTag, Literal(" ⊗ "), rightTag, Literal(")"))),PROV_COL)()
          val newJoin = j.copy(left = taggedLeft, right = taggedRight)
          val cleanedOutput = newJoin.output.filter(_.name != PROV_COL)

          Project(cleanedOutput :+ combinedTag, newJoin)
          
        }
  
      // We look for 'Aggregate' nodes, which represent Aggregate statements
      case a @ Aggregate(groupingExprs, aggExprs, child, hint) if (hasProv(child)) =>
        if (hasProv(a)) {
          a
        } else {
          val taggedChild = ensureProv(child)
          val childTag = getProvAttr(child).get

          val collectSet = AggregateExpression(CollectSet(childTag), Complete, isDistinct = false)
          val combinedTag = Alias(Concat(Seq(Literal("{"), ConcatWs(Seq(Literal(" ⊕ "), collectSet)), Literal("}"))), PROV_COL)()
          a.copy(child = taggedChild, aggregateExpressions = aggExprs :+ combinedTag)
        }

      case f @ Filter(condition, child) if (hasProv(child)) => f

      case s @ Sort(order, global, child, hint)  => s
                
      
    }
  }
}
