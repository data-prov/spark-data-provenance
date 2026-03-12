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
