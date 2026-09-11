package org.dataprov.dp.wringlet

import org.scalatest.funspec.AnyFunSpec

import org.dataprov.dp.wringlet.ProvenanceApi._

trait ProvenanceModeTestUtils { this: AnyFunSpec with SparkSessionTestWrapper =>

  protected def itWithProvenanceEnabled(testName: String)(testBody: => Any): Unit =
    it(testName) {
      withProvenanceEnabled(spark) {
        testBody
      }
    }

  protected def itWithProvenanceEnabled(testName: String, provenanceColName: String)(testBody: => Any): Unit =
    it(testName) {
      withProvenanceEnabled(spark, provenanceColName) {
        testBody
      }
    }

  protected def itWithProvenanceDisabled(testName: String)(testBody: => Any): Unit =
    it(testName) {
      withProvenanceDisabled(spark) {
        testBody
      }
    }
  
    protected def itWithProvenanceDisabled(testName: String, provenanceColName: String)(testBody: => Any): Unit =
    it(testName) {
      withProvenanceDisabled(spark, provenanceColName) {
        testBody
      }
    }
}
