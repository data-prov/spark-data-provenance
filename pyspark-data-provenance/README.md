# pyspark-data-provenance

Python package in this monorepo for enabling Spark data provenance features in PySpark jobs.

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

## Common Commands

* `make init` - Create/refresh lock file and sync dependencies
* `make update` - Upgrade dependencies and sync environment
* `make check` - Run lint and type checks
* `make test` - Run unit tests
* `make build` - Build package wheel
* `make publish` - Publish package (PyPI)

## PyPI Package

The Python package is published on PyPI:

https://pypi.org/project/pyspark-data-provenance/

You can install it with pip:

```bash
pip install pyspark-data-provenance
```

Before publishing to production PyPI with `make publish`, set your token:

```bash
export UV_PUBLISH_TOKEN=<your-pypi-token>
```

For details, see the
[uv documentation](https://docs.astral.sh/uv/guides/package/#publishing-your-package)

## CI/CD

Repository-level workflows are provided for:

* CI: lint, type-check, and tests on pushes/PRs affecting this package
* Publish: build and publish on release
