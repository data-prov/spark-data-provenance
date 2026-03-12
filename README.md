# Fine-grained data provenance for Spark

## Table of Content (ToC)

* [Fine\-grained data provenance for Spark](#fine-grained-data-provenance-for-spark)
  * [Table of Content (ToC)](#table-of-content-toc)
  * [Overview](#overview)
  * [References](#references)
  * [Getting started](#getting-started)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)

## Overview

This [project](https://github.com/data-prov/spark-data-provenance)
explores how Spark may be instrumented/complemented with fine-grained provenance
features.

Even though the members of the GitHub organization may be employed by
some companies, they speak on their personal behalf and do not represent
these companies.

## References

* [GitHub - Data Provenance - Spark (this Git repository)](https://github.com/data-prov/spark-data-provenance)
* [Pypi.org - pyspark-data-provenance package](https://pypi.org/project/pyspark-data-provenance/)
* [Data Engineering Helpers - Knowledge Sharing - Python](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/programming/python/)
* [Data Engineering Helpers - Knowledge Sharing - Jupyter, PySpark and DuckDB](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/programming/jupyter/jupyter-pyspark-duckdb/)
* [Data Engineering Helpers - Knowledge Sharing - Spark](https://github.com/data-engineering-helpers/ks-cheat-sheets/blob/main/data-processing/spark/)

## Getting started

* Build the Scala Spark JAR:

```bash
make scala-build
```

* Build the Python wheel:

```bash
make python-build
```

* Publish the Python wheel onto
  [Pypi.org](https://pypi.org/project/pyspark-data-provenance/)
  * The `UV_PUBLISH_TOKEN` environment variable needs to be specified
  (see the
  [uv documentation](https://docs.astral.sh/uv/guides/package/#publishing-your-package)
  for further details)

```bash
make python-publish
```

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
