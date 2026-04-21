package org.dataprov.dp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Join, LeafNode}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, MonotonicallyIncreasingID, Multiply}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan


case class AddProvenanceColumn(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val isEnabled = spark.sessionState.conf.getConfString("spark.provenance.enabled", "false")
    if (isEnabled != "true") {
      return plan // If the feature is not enabled, return the plan unchanged
    }

    // transformUp traverses the tree from the bottom leaves to the top root
    plan.transformUp {
      
      // We look for 'Project' nodes, which represent SELECT statements
      
      case p @ Project(projectList, child) =>

        // CRITICAL: Catalyst runs rules repeatedly until the plan stops changing.
        // We must check if we already added our column to avoid an infinite loop!
        if (projectList.exists(_.name == "_provenance_tag_select")) {
          p // Return the node unchanged
        } else {
          // Create a new literal string column named '_provenance_tag_select'
          val provenanceCol = Alias(MonotonicallyIncreasingID(), "_provenance_tag_select")()
          
          // Return a new Project node with our column appended to the list
          Project(projectList :+ provenanceCol, child)
        }
      

    

      
      // We look for 'Join' nodes, which represent JOIN statements
      case j @ Join(left, right, joinType, condition, hint) =>
        if (j.output.exists(_.name == "_provenance_tag_select")) {
          j // Return the node unchanged
        } else {
            // We search the provenance tag in each
          val leftTag = j.left.output.find(_.name == "_provenance_tag_select")
          val rightTag = j.right.output.find(_.name == "_provenance_tag_select")

          (leftTag, rightTag) match {
            case (Some(l), Some(r)) =>
              // We create a new literal string column named '_provenance_tag_select' 
              // It corresponds to leftTag * rightTag
              val multipliedTag = Alias(Multiply(l, r), "_provenance_tag_join")()

              // Create a new projectList without tag column
              val projectList = j.output.filter(_.name != "_provenance_tag_select")

              // Return a new Project node with our column appended to the list
              Project(projectList :+ multipliedTag, j)
            
            // If there is no leftTag 
            case (Some(l), None) => 
              print("case (l,null)")
              val projectList = j.output.filter(_.name != "_provenance_tag_join")
              Project(projectList :+ Alias(l, "_provenance_tag_select")(), j)


            // If there is no rightTag
            case (None, Some(r)) => 
              print("Case (null,r)")
              val projectList = j.output.filter(_.name != "_provenance_tag_join")
              Project(projectList :+ Alias(r, "_provenance_tag_select")(), j)

            // If there is no tag at all
            case _ => j
            
          }
        
        }
      
      
    }
  }
}
