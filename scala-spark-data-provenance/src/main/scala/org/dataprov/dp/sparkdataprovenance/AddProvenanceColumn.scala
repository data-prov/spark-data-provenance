package org.dataprov.dp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Join, Filter, Aggregate}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, MonotonicallyIncreasingID, Multiply}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Sum, AggregateExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan

case class AddProvenanceColumn(spark: SparkSession) extends Rule[LogicalPlan] {


  override def apply(plan: LogicalPlan): LogicalPlan = {
    val isEnabled = spark.sessionState.conf.getConfString("spark.provenance.enabled", "false")
    if (isEnabled != "true") {
      return plan // If the feature is not enabled, return the plan unchanged
    }

    val PROV_COL = "_provenance_taged"

    // Check if a plan already has provenance propagated
    def hasProv(plan: LogicalPlan): Boolean = 
      plan.output.exists(_.name == PROV_COL)

    // Find the provenance attribute in a plan
    def getProvAttr(plan: LogicalPlan) = 
      plan.output.find(_.name == PROV_COL)

    // transformUp traverses the tree from the bottom leaves to the top root
    plan.transformUp {
      
      // We look for 'Project' nodes, which represent SELECT statements
      
      case p @ Project(projectList, child) =>

        // CRITICAL: Catalyst runs rules repeatedly until the plan stops changing.
        // We must check if we already added our column to avoid an infinite loop!
        if (projectList.exists(_.name == PROV_COL)) {
          p // Return the node unchanged
        } else {
          // Create a new literal string column named '_provenance_tag_select'
          val provenanceCol = Alias(MonotonicallyIncreasingID(), PROV_COL)()
          
          // Return a new Project node with our column appended to the list
          Project(projectList :+ provenanceCol, child)
        }
      
      
      // We look for 'Join' nodes, which represent JOIN statements
      case j @ Join(left, right, joinType, condition, hint) if (hasProv(left) && hasProv(right)) =>
        if (hasProv(j)) {
          j // Return the node unchanged
        } else {
            // We search the provenance tag in each
          val leftTag = getProvAttr(j.left)
          val rightTag = getProvAttr(j.right)

          (leftTag, rightTag) match {
            case (Some(l), Some(r)) =>
              // We create a new literal string column named '_provenance_tag_select' 
              // It corresponds to leftTag * rightTag
              val multipliedTag = Alias(Multiply(l, r), PROV_COL)()

              // Create a new projectList without tag column
              val projectList = j.output.filter(_.name != PROV_COL)

              // Return a new Project node with our column appended to the list
              Project(projectList :+ multipliedTag, j)
            
            // If there is no leftTag 
            case (Some(l), None) => 
              val projectList = j.output.filter(_.name != PROV_COL)
              Project(projectList :+ Alias(l, PROV_COL)(), j)


            // If there is no rightTag
            case (None, Some(r)) => 
              val projectList = j.output.filter(_.name != PROV_COL)
              Project(projectList :+ Alias(r, PROV_COL)(), j)

            // If there is no tag at all
            case _ => j
            
          }
        
        }

      case f @ Filter(condition, child) if (hasProv(child)) =>
        if (hasProv(f)) f
        else {
          f
        }  

      case a @ Aggregate(groupingExprs, aggExprs, child, hint) if (hasProv(child)) =>
        if (aggExprs.exists(_.name == PROV_COL)) {
          a
        } else {
          val childTag = getProvAttr(child).get
          val provSum = Alias(AggregateExpression(Sum(childTag), Complete, false), PROV_COL)()
          a.copy(aggregateExpressions = aggExprs :+ provSum)
        } 
      
      
    }
  }
}
