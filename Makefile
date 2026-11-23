GRADLEW := ./gradlew
PROJECT_VERSION := $(shell grep -E '^\s*version\s*=' build.gradle.kts | sed -E 's/.*"([^"]+)".*/\1/')
IMAGE_NAME := data-extraction

NAME ?= world
SLOT ?=
PUBLICATION ?=
TABLE_INCLUDE_LIST ?=
STREAMING_NETWORK ?= streaming_streaming
AVRO_TOOLS_JAR ?= avro-tools-1.11.3.jar
ARGS ?=

CONFIG_DIR := $(CURDIR)/config
SNAPSHOT_PROPERTIES_DOCKER := $(CONFIG_DIR)/snapshot.properties
SNAPSHOT_PROPERTIES_HOST := $(CONFIG_DIR)/snapshot.host.properties
SNAPSHOT_PROPERTIES_CONTAINER := /config/snapshot.properties
DATA_DIR := /opt/data/raw
RAW_DATA_DIR := /opt/data/raw

.PHONY: build test clean build-docker hello postgres-meta postgres-meta-docker snapshot snapshot-docker cdc-replicate cdc-replicate-docker avro-tools

$(GRADLEW):
	@test -x $(GRADLEW) || (echo "gradlew is missing or not executable"; exit 1)

build: $(GRADLEW)
	$(GRADLEW) build

test: $(GRADLEW)
	$(GRADLEW) test

clean: $(GRADLEW)
	$(GRADLEW) clean

build-docker:
	docker build -t $(IMAGE_NAME):$(PROJECT_VERSION) .

hello: $(GRADLEW)
	SPRING_SHELL_INTERACTIVE_ENABLED=false $(GRADLEW) bootRun --args="hello --name $(NAME)"

postgres-meta: $(GRADLEW)
	SPRING_SHELL_INTERACTIVE_ENABLED=false $(GRADLEW) bootRun --args="postgres-meta$(if $(SLOT), --slot $(SLOT))"

postgres-meta-docker: build-docker
	docker run --rm \
		--network $(STREAMING_NETWORK) \
		-e SPRING_SHELL_INTERACTIVE_ENABLED=false \
		-e MANAGEMENT_OTLP_METRICS_EXPORT_URL=http://otel-collector:4318/v1/metrics \
		-e MANAGEMENT_OTLP_TRACING_ENDPOINT=http://otel-collector:4318/v1/traces \
		$(IMAGE_NAME):$(PROJECT_VERSION) \
		postgres-meta$(if $(SLOT), --slot $(SLOT))

snapshot: $(GRADLEW)
	@mkdir -p build/data-extraction
	SPRING_SHELL_INTERACTIVE_ENABLED=false \
		SPRING_CONFIG_ADDITIONAL_LOCATION=file:$(SNAPSHOT_PROPERTIES_HOST) \
		$(if $(PUBLICATION),SOURCE_DATABASE_PUBLICATION_NAME=$(PUBLICATION)) \
		$(if $(SLOT),SOURCE_DATABASE_SLOT_NAME=$(SLOT)) \
		$(GRADLEW) bootRun --args="cdc-snapshot"

snapshot-docker: build-docker
	@mkdir -p $(DATA_DIR)
	docker run --rm \
		--sig-proxy \
		--stop-timeout 30 \
		--init \
		--network $(STREAMING_NETWORK) \
		-v $(CONFIG_DIR):/config:ro \
		-v $(DATA_DIR):$(RAW_DATA_DIR) \
		-e SPRING_SHELL_INTERACTIVE_ENABLED=false \
		-e SPRING_CONFIG_ADDITIONAL_LOCATION=file:$(SNAPSHOT_PROPERTIES_CONTAINER) \
		$(if $(PUBLICATION),-e SOURCE_DATABASE_PUBLICATION_NAME=$(PUBLICATION)) \
		$(if $(SLOT),-e SOURCE_DATABASE_SLOT_NAME=$(SLOT)) \
		$(if $(TABLE_INCLUDE_LIST),-e SOURCE_DATABASE_TABLEINCLUDELIST='$(TABLE_INCLUDE_LIST)') \
		-e MANAGEMENT_OTLP_TRACING_ENDPOINT=http://otel-collector:4318/v1/traces \
		-e MANAGEMENT_OTLP_METRICS_EXPORT_URL=http://otel-collector:4318/v1/metrics \
		$(IMAGE_NAME):$(PROJECT_VERSION) \
		cdc-snapshot

cdc-replicate: $(GRADLEW)
	@mkdir -p build/data-extraction
	SPRING_SHELL_INTERACTIVE_ENABLED=false \
		SPRING_CONFIG_ADDITIONAL_LOCATION=file:$(SNAPSHOT_PROPERTIES_HOST) \
		$(if $(PUBLICATION),SOURCE_DATABASE_PUBLICATION_NAME=$(PUBLICATION)) \
		$(if $(SLOT),SOURCE_DATABASE_SLOT_NAME=$(SLOT)) \
		$(GRADLEW) bootRun --args="cdc-replicate"

cdc-replicate-docker: build-docker
	@mkdir -p $(DATA_DIR)
	docker run --rm \
		--sig-proxy \
		--stop-timeout 30 \
		--init \
		--network $(STREAMING_NETWORK) \
		-v $(CONFIG_DIR):/config:ro \
		-v $(DATA_DIR):$(RAW_DATA_DIR) \
		-e SPRING_SHELL_INTERACTIVE_ENABLED=false \
		-e SPRING_CONFIG_ADDITIONAL_LOCATION=file:$(SNAPSHOT_PROPERTIES_CONTAINER) \
		$(if $(PUBLICATION),-e SOURCE_DATABASE_PUBLICATION_NAME=$(PUBLICATION)) \
		$(if $(SLOT),-e SOURCE_DATABASE_SLOT_NAME=$(SLOT)) \
		$(if $(TABLE_INCLUDE_LIST),-e SOURCE_DATABASE_TABLEINCLUDELIST='$(TABLE_INCLUDE_LIST)') \
		-e MANAGEMENT_OTLP_TRACING_ENDPOINT=http://otel-collector:4318/v1/traces \
		-e MANAGEMENT_OTLP_METRICS_EXPORT_URL=http://otel-collector:4318/v1/metrics \
		$(IMAGE_NAME):$(PROJECT_VERSION) \
		cdc-replicate

avro-tools:
	java -jar $(AVRO_TOOLS_JAR) $(ARGS)
