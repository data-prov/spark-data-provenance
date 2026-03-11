# Load shared and environment-specific variables for make targets.
ENV ?= preprod
ENV_DIR := env
ENV_BASE_FILE := $(ENV_DIR)/.env.base
ENV_FILE := $(ENV_DIR)/.env.$(ENV)

include $(ENV_BASE_FILE)
include $(ENV_FILE)

# Build project
build:
	uv build --wheel

publish:
	$(eval CA_TOKEN=$(shell aws codeartifact get-authorization-token --region=$(AWS_REGION) \
		--domain=$(CA_DOMAIN) --domain-owner=$(CA_OWNER) --query authorizationToken \
		--output text --duration-seconds 43200))
	$(eval UV_INDEX_DKTUNITED_PASSWORD=$(CA_TOKEN))
	uv publish --username=$(CA_USER) --password=$(CA_TOKEN) --publish-url=$(CA_URL)