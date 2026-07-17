# Scala Spark - Fine-grained data provenance - Scala part

## Table of Content (ToC)

* [Scala Spark \- Fine\-grained data provenance \- Scala part](#scala-spark---fine-grained-data-provenance---scala-part)
  * [Table of Content (ToC)](#table-of-content-toc)
  * [Overview](#overview)
  * [Architecture & Provenance Builders](#architecture--provenance-builders)
  * [Supported Operators](#supported-operators)
  * [Backward Lineage & Minimal Datasets](#backward-lineage--minimal-datasets)
  * [Quick Start](#quick-start)
  * [Project Layout](#project-layout)
  * [Common commands](#common-commands)
  * [Development life\-cycle](#development-life-cycle)
  * [JAR Distribution and Deployment](#jar-distribution-and-deployment)
  * [CI/CD](#cicd)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)


## Overview

Scala package in this monorepo implementing custom logical plan optimization rules (`Rule[LogicalPlan]`) within **Spark Catalyst** to enable data provenance features.

This project is managed with `sbt` (Scala Build Tool).

## Architecture and Provenance Builders

The core engine is built around a **Pluggable Provenance Architecture**. The traversal of the Catalyst logical plan is decoupled from the actual formatting of the provenance tags using the `ProvenanceBuilder` interface. 

You can easily switch between different provenance semantics without altering the core engine:
* **Boolean Provenance**: Tracks the presence/absence of source records.
* **Display String Provenance**: Provide a human-readable text-based representation of the full lineage path
  with academic operators, without evaluation.
* **Why Provenance (Full-Why)**: Exhaustively tracks all contributing source records for a given output row.
* **Semi-Why Provenance**: Provides a compacted or flattened version of the lineage to reduce memory footprint on massive datasets.

## Supported Operators

The engine supports end-to-end provenance tracking across a wide range of Spark SQL and DataFrame APIs:
* **Basic Operations**: `Project` (with aliasing), `Filter` (AND, OR, NOT, IS NULL), `Sort`.
* **Joins**: `Inner`, `Outer`, `Left`, `Right`, `Cross`, `Left Semi Join` and `Left Anti Join`.
* **Aggregations**: `Aggregate` nodes with advanced witness election (`MIN`, `MAX`, `SUM`, `AVG`).
* **Set Operations**: `Union`, `Intersect`, `Except`.
* **Deduplication**: `Distinct` (SQL) and `dropDuplicates()` (DataFrame).
* **Analytics**: `Window` functions.

## Backward Lineage and Minimal Datasets

The framework includes backward lineage capabilities allowing users to query final pipeline results and retrieve the almost **minimal input datasets** that contributed to those specific rows. This turns abstract provenance tags into concrete, actionable source data debugging.

---

## Quick Start

From the repository root:

```bash
cd scala-spark-data-provenance
make clean           
make init           
make check           
make test 
make build
```

## Project Layout

```text
scala-spark-data-provenance/
    project/
    src/main/scala/org/dataprov/dp/sparkdataprovenance/
    src/test/scala/org/dataprov/dp/sparkdataprovenance/
    target/
    tests/
    build.sbt
    Makefile
```


## Common commands

* `make info` - Display the current package version and targeted Scala minor version
* `make clean` - Remove compilation caches, `target/` directories, and temporary SBT project artifacts
* `make compile` - Compile the Scala source code files
* `make build` - Package the project into a standard library JAR file (`sbt package`) and display its path
* `make test` - Run unit test
* `make publish-local` - Publish the package as a local Snapshot artifact to both your Ivy local cache (`publishLocal`) and Maven local repository (`publishM2`).
* `make lint` - Verify semantic and syntactic rules using Scalafix without modifying files
* `make fix-lint` - Automatically apply code fixes and rewrites based on Scalafix rules
* `make format-check` - Check if the Scala source files comply with style guidelines using Scalafmt
* `make format` - Automatically reformat all Scala source files according to rules
* `make check` - Run a complete quality gate validation by executing both linting and formatting checks (`make lint format-check`)
* `make fix` - Automatically apply all available formatting and linting fixes to the codebase (`make fix-lint format`)

## JAR Distribution and Deployment

The core engine is packaged as a JAR to allow native injection inside Spark 
environments without manual dependency resolution.

Optionally, the artifact can be compiled and verified locally:

```bash
make build
make publish-local
```

The resulting file is generated under `target/scala-2.13/dp-spark-assembly-[VERSION].jar`.

## Cluster Deployment 

To enable the provenance rules inside an enterprise cloud environment like 
Databricks, the JAR must be loaded onto the driver's classpath at boot 
time using an Initialization Script (Init Script)

Upload the generated assembly JAR to your Databricks DBFS or Workspace Volume.

Configure a cluster Init Script (install_provenance_jar.sh) to link the JAR

Restart the cluster nodes to trigger the Rule[LogicalPlan] interception.


## CI/CD

Repository-level workflows are provided for:

* CI: lint, format-check, and tests on pushes/PRs affecting this package via `make check`
* Publish: build and publish-local on release
