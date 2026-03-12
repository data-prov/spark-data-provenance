# Fine-grained data provenance for Spark

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
