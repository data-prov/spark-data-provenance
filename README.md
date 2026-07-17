# Fine-grained data provenance for Spark

## Table of Content (ToC)

* [Fine\-grained data provenance for Spark](#fine-grained-data-provenance-for-spark)
  * [Table of Content (ToC)](#table-of-content-toc)
  * [Overview](#overview)
  * [Core Architecture and Features](#core-architecture-and-features)
    * [Supported Provenance Builders (Modalities)](#supported-provenance-builders-modalities)
      * [Boolean Provenance](#1-boolean-provenance)
      * [Display Provenance](#2-display-provenance-default)
      * [Full Why-Provenance](#3-full-why-provenance)
      * [Semi Why-Provenance](#4-semi-why-provenance-selective)
    * [Supported SQL Operators](#supported-sql-operators)
    * [Backward Lineage and Minimal Datasets](#backward-lineage-and-minimal-datasets)
  * [References](#references)
  * [Pre-requisites](#pre-requisites)
  * [Getting started](#getting-started)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)

## Overview

This [project](https://github.com/data-prov/spark-data-provenance)
explores how Spark may be instrumented/complemented with fine-grained provenance
features.

In industrial Big Data pipelines, testing and debugging transformations on 
massive datasets is both financially costly and computationally heavy. 
This extension solves this friction point by performing backward lineage tracing 
directly inside Spark. By identifying the exact source rows (tuples) that 
contributed to a specific query output, it allows data engineers to automatically 
isolate an almost **minimal, functional, and consistent slice of input data** ideal for 
rapid local testing, prototyping, and debugging.

Even though the members of the GitHub organization may be employed by
some companies, they speak on their personal behalf and do not represent
these companies.

## Core Architecture and Features

This tool hooks into **Spark Catalyst**, which is Spark's native query optimizer, 
by injecting custom tree-rewriting rules extended from `Rule[LogicalPlan]`. 
To guarantee absolute stability and avoid breaking internal operations (such as 
native `.show()`), the provenance rules are resolved **post-hoc** once the 
original logical tree is fully analyzed and stabilized.

### Supported Provenance Builders (Modalities)

To balance the trade-off between tracking precision and execution performance, 
the extension introduces a modular design based on **Provenance Builders**. 
Users can configure the context to use one of four tracking modalities depending 
on the target use case:

| Modality / Builder | Data Type | Performance Overhead | Primary Use Case |
| :--- | :--- | :--- | :--- |
| **Boolean Provenance** | `Boolean` | Minimal | Data auditing, sanity checks, validation of row activation. |
| **Display Provenance** | `String` | High | Interactive debugging, lineage visualization in Jupyter/Databricks notebooks. |
| **Full Why-Provenance** | `Array[Array[Tag]]` | High | Academic research, exhaustive impact analysis, complex multi-path tracking. |
| **Semi-Why Provenance** | `Array[Tag]` | Balanced (Optimized) | Automated minimal test dataset generation, local prototyping. |

#### 1. Boolean Provenance
* **Concept:** The simplest form of lineage. It evaluates whether a source row was 
  processed and actively participated in the generation of at least one output row.
* **Behavior:** Instead of tracking IDs or execution paths, it propagates a binary 
  tag (`True` / `False`). 
* **Benefit:** It provides the mathematical foundation to solve Incremental 
  View Maintenance problem. When upstream rows are deleted (their variables are set 
  to `False`), Spark can instantly evaluate the Boolean expression. If it evaluates
  to `False`, the corresponding output row is purged automatically, avoiding a costly
  and heavy full recomputation of the pipeline.

#### 2. Display Provenance (Default)
* **Concept:** A human-readable text-based representation of the full lineage path
  with academic operators, without evaluation.
* **Behavior:** It aggregates the operations and source identifiers into a 
  formatted string block appended to the processed data.
* **Benefit:** Tailored for the developer experience. It allows users to quickly 
  run a `.show()` inside a Databricks or Jupyter notebook to visually trace a 
  suspicious data point back to its root table name or partition without querying 
  low-level metadata.

#### 3. Full Why-Provenance
* **Concept:** The classic academic implementation of Why-Provenance based on 
  relational semirings.
* **Behavior:** It builds a complete combinatorial tree of all alternative records 
  that could explain the existence of a given output row. If an output row is the 
  result of a heavily aggregated dataset or a global deduplication (`DISTINCT`),
  it embeds every single matching row variant inside a multi-dimensional array of arrays.
* **Benefit:** Guarantees absolute, non-lossy lineage tracking for strict regulatory
  or forensic compliance requirements.

#### 4. Semi-Why Provenance (Selective)
* **Concept:** The default, pragmatic modality engineered specifically for this project's 
  core requirement.
* **Behavior:** It simplifies the standard Why-Provenance paradigm by executing a distinct 
  filtration pass. Instead of collecting every mathematical permutation of a derivation, 
  it drops redundant alternative lineages and preserves almost only the minimum sufficient rows
  required to successfully re-trigger and test the operator's logic.
* **Benefit:** Flattens the memory footprint into a clean, one-dimensional `Array[Tag]`, 
  ensuring that Spark Catalyst can optimize the abstract syntax tree (AST) swiftly while 
  extracting the lightest possible dataset slice for local engineering unit tests.

### Supported SQL Operators
The extension successfully propagates provenance tags across major relational 
algebra operators:
*   **Projection / Selection:** `SELECT` (`Project`), `FILTER` (AND, OR, NOT, IS NULL)
*   **Sorting & Deduplication:** `ORDER BY` (`Sort`), `DISTINCT`
*   **Aggregations:** `GROUP BY` (`Aggregate` with specific witness election like `MIN`, `MAX`, `SUM`, `AVG`)
*   **Jointures:** `JOIN` (`Inner`, `Left Outer`, `Right Outer`, `Full Outer`, `Cross`, `Left Semi Join` and `Left Anti Join`)
*   **Set Operations:** `UNION`, `INTERSECT`, `EXCEPT`
*   **Analytics:** `Window` functions

### Backward Lineage and Minimal Datasets
The framework includes a dedicated backward lineage extraction function. 
By targeting specific rows at the very end of a complex pipeline, the framework 
can trace the tags back to their roots and automatically extract the 
**minimal input datasets** (not completely minimal). This effectively turns 
abstract provenance arrays into concrete, actionable source data for local debugging.


## References

* [GitHub - Data Provenance - Spark (this Git repository)](https://github.com/data-prov/spark-data-provenance)
* [Pypi.org - pyspark-data-provenance package](https://pypi.org/project/pyspark-data-provenance/)
* [Data Engineering Helpers - Knowledge Sharing - Java](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/programming/java-world/)
* [Data Engineering Helpers - Knowledge Sharing - Python](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/programming/python/)
* [Data Engineering Helpers - Knowledge Sharing - Jupyter, PySpark and DuckDB](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/programming/jupyter/jupyter-pyspark-duckdb/)
* [Data Engineering Helpers - Knowledge Sharing - Spark](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/data-processing/spark/)

### Summary

* [Scala package](https://github.com/data-prov/spark-data-provenance/blob/main/scala-spark-data-provenance/)
  * [Scala package - GitHub - Specified Scala version](https://github.com/data-prov/spark-data-provenance/blob/main/scala-spark-data-provenance/SCALA_MINOR_VERSION)
  * [Scala package - GitHub - Specified package version](https://github.com/data-prov/spark-data-provenance/blob/main/scala-spark-data-provenance/VERSION)
* [Python package](https://github.com/data-prov/spark-data-provenance/blob/main/pyspark-data-provenance/)
  * [Python package - GitHub - Specified Python version](https://github.com/data-prov/spark-data-provenance/blob/main/pyspark-data-provenance/.python-version)
  * [Python package - GitHub - Specified package version](https://github.com/data-prov/spark-data-provenance/blob/main/pyspark-data-provenance/VERSION)
  * [Python package - Pypi.org - Published package (`pyspark-data-provenance`)](https://pypi.org/project/pyspark-data-provenance/)

## Pre-requisites

* For the JVM-related tooling (_e.g._, sbt, Scala, Spark), the easiest is
  to install them with SDKMan. All the details are provided in
  [Data Engineering Helpers - Knowledge Sharing - Java](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/programming/java-world/)
  * JDK
    * Example of how to install an open JDK with SDKMan
  ([Amazon Corretto 21](https://docs.aws.amazon.com/corretto/latest/corretto-21-ug/what-is-corretto-21.html)
  in this case): `sdk install java 21.0.10-amzn`
  * sbt
    * Example of how to install sbt with SDKMan:
  `sdk install sbt 1.12.5`
  * Scala
    * The Scala (minor) version is specified in the
  [`scala-spark-data-provenance/SCALA_MINOR_VERSION` file](https://github.com/data-prov/spark-data-provenance/blob/main/scala-spark-data-provenance/SCALA_MINOR_VERSION)
    * Example of how to install Scala with SDK: `sdk install scala 2.13.18`
* For the Python-related tooling (_e.g._, PySpark, Jupyter), the easiest is
  to install them with the native Python packages (_e.g._, on MacOS/Linux)
  and the native uv package.
  All the details are provided in
  [Data Engineering Helpers - Knowledge Sharing - Python](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/programming/python/)
  * Python native packages
    * The Scala (minor) version is specified in the
  [`pyspark-data-provenance/.python-version` file](https://github.com/data-prov/spark-data-provenance/blob/main/pyspark-data-provenance/.python-version)
    * Example of how to install Python 3.12 on MacOS: `brew install python@3.12`
  * uv
    * Example of how to install uv on MacOS: `brew install uv`

## Getting started

* Build the Scala Spark JAR:

```bash
make scala-build
```

* If needed, install Python with uv (that needs to be done only once):

```bash
make python-init-uv-python
```

* Potentially bump the
  [version of the Python package](https://github.com/data-prov/spark-data-provenance/blob/main/pyspark-data-provenance/VERSION)
  with either of the following options (ordered by the general probability of
  occurrence in the development life-cycle, from the highest to the lowest)
  * Increment the dev version (_e.g._, from `2.4.3.dev5` to `2.4.3.dev6` or
  from `2.4.3` to `2.4.4.dev0`):
  `make python-increment-dev-version`
  * Bump to minor version (_e.g._, from `2.4.3.dev5` to `2.4.3`):
  `make python-bump-to-minor-version`
  * Bump to patch version (_e.g._, from `2.4.3.dev5` to `2.4.4`):
  `make python-bump-to-patch-version`
  * Bump to major version (_e.g._, from `2.4.3.dev5` to `3.0.0`):
  `make python-bump-to-major-version`

* Build the Python wheel:

```bash
make python-build
```

* Optionally, the Python wheel may be published manually onto
  [Pypi.org](https://pypi.org/project/pyspark-data-provenance/)
  * The `UV_PUBLISH_TOKEN` environment variable then needs to be specified
  (see the
  [uv documentation](https://docs.astral.sh/uv/guides/package/#publishing-your-package)
  for further details)

```bash
make python-publish
```

* However, the easiest way to publish the Python wheel is through the CI/CD
  pipeline, that is, the
  [GitHub Actions Python publishing pipeline](https://github.com/data-prov/spark-data-provenance/actions/workflows/python-publish.yml).
  * That CI/CD pipeline is automatically triggered when creating a release on
  the Git repository
  * It may also be triggered manually by contributors of the Git repository

* Check the versions of the Python wheel on
  [Pypi.org](https://pypi.org/project/pyspark-data-provenance/)

* Install the Python wheel locally, in the main/global Python environment
  (as `spark-submit` does not seem to work properly with uv):

```bash
make python-install-local
```

* Run the PySpark job locally, in the main/global Python environment
  (as `spark-submit` does not seem to work properly with uv):

```bash
make python-run-local
```