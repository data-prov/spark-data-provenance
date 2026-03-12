# PySpark - Fine-grained data provenance - PySpark part

## Table of Content (ToC)

* [PySpark \- Fine\-grained data provenance \- PySpark part](#pyspark---fine-grained-data-provenance---pyspark-part)
  * [Table of Content (ToC)](#table-of-content-toc)
  * [Overview](#overview)
  * [Quick Start](#quick-start)
  * [Project Layout](#project-layout)
  * [Common commands](#common-commands)
  * [Development life\-cycle](#development-life-cycle)
  * [PyPI package](#pypi-package)
  * [CI/CD](#cicd)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)

## Overview

Python package in this monorepo for enabling Spark data provenance features
in PySpark jobs.

This project is managed with `uv`.

## Quick Start

From the repository root:

```bash
cd pyspark-data-provenance
make init-uv-python
make init
make check
make test
make run
```

## Project Layout

```text
pyspark-data-provenance/
    src/pyspark_data_provenance/
    tests/
    main.py
    pyproject.toml
    Makefile
```

## Common commands

* `make init` - Create/refresh lock file and sync dependencies
* `make update` - Upgrade dependencies and sync environment
* `make check` - Run lint and type checks
* `make test` - Run unit tests
* `make build` - Build package wheel
* `make publish` - Publish package (PyPI)

## Development life-cycle

* In order to switch (bump) to a newer
  [version of the Python package](https://github.com/data-prov/spark-data-provenance/blob/main/pyspark-data-provenance/VERSION),
  the following are the main options, ordered by the general probability of
  occurrence in the development life-cycle, from the highest to the lowest)
  * Increment the dev version (_e.g._, from `2.4.3.dev5` to `2.4.3.dev6` or
  from `2.4.3` to `2.4.4.dev0`):
  `make increment-dev-version`
  * Bump to minor version (_e.g._, from `2.4.3.dev5` to `2.4.3`):
  `make bump-to-minor-version`
  * Bump to patch version (_e.g._, from `2.4.3.dev5` to `2.4.4`):
  `make bump-to-patch-version`
  * Bump to major version (_e.g._, from `2.4.3.dev5` to `3.0.0`):
  `make bump-to-major-version`
* Then, the version bump has to be cascaded to related files (typically,
  [`pyproject.toml`](https://github.com/data-prov/spark-data-provenance/blob/main/pyspark-data-provenance/pyproject.toml))

## PyPI package

* The
  [Python package is published on PyPI](https://pypi.org/project/pyspark-data-provenance/)

* Optionally, the Python wheel may be published manually onto
  [Pypi.org](https://pypi.org/project/pyspark-data-provenance/)
  * The `UV_PUBLISH_TOKEN` environment variable then needs to be specified
  (see the
  [uv documentation](https://docs.astral.sh/uv/guides/package/#publishing-your-package)
  for further details)

```bash
make publish
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
make install-local
```

* For information, that actually install the Python wheel with pip:

```bash
python -mpip install -U pyspark-data-provenance
```

* Run the PySpark job locally, in the main/global Python environment
  (as `spark-submit` does not seem to work properly with uv):

```bash
make run-local
```

## CI/CD

Repository-level workflows are provided for:

* CI: lint, type-check, and tests on pushes/PRs affecting this package
* Publish: build and publish on release
