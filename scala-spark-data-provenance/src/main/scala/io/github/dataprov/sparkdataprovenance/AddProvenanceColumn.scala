package io.github.dataprov.sparkdataprovenance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, MonotonicallyIncreasingID}
import org.apache.spark.sql.catalyst.rules.Rule


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
        if (projectList.exists(_.name == "_provenance_id")) {
          p // Return the node unchanged
        } else {
          // Create a new literal string column named '_provenance_id'
          val provenanceCol = Alias(MonotonicallyIncreasingID(), "_provenance_id")()
          
          // Return a new Project node with our column appended to the list
          Project(projectList :+ provenanceCol, child)
        }
    }
  }
}
