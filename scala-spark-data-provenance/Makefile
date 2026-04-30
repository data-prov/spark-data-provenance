#
# https://github.com/data-prov/spark-data-provenance/blob/main/scala-spark-data-provenance/Makefile
#
SCALA_MINOR_VERSION ?= $(shell cat SCALA_MINOR_VERSION)
SCALA_PACKAGE_NAME ?= dp-spark
SCALA_PACKAGE_VERSION ?= $(shell cat VERSION)
SCALA_PACKAGE_JAR ?= $(SCALA_PACKAGE_NAME)_$(SCALA_MINOR_VERSION)-$(SCALA_PACKAGE_VERSION).jar

.PHONY: info clean compile build test publish-local lint fix-lint format-check format check fix

info:
	@echo "Package version: $(SCALA_PACKAGE_VERSION) - Scala version: $(SCALA_MINOR_VERSION)"

clean:
	rm -rf target project/project project/target

# Build project
compile:
	sbt compile

build:
	sbt package
	ls -lFh target/scala-$(SCALA_MINOR_VERSION)/$(SCALA_PACKAGE_JAR)

test:
	sbt test

publish-local:
	sbt 'set isSnapshot := true' publishLocal publishM2

lint:
	sbt 'scalafix --check'

fix-lint:
	sbt scalafix

format-check:
	sbt scalafmtCheck

format:
	sbt scalafmt

check: lint format-check

fix: fix-lint format
