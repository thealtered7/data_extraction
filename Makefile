GRADLEW := ./gradlew
PROJECT_VERSION := $(shell grep -E '^\s*version\s*=' build.gradle.kts | sed -E 's/.*"([^"]+)".*/\1/')
IMAGE_NAME := data-extraction

NAME ?= world
SLOT ?=
STREAMING_NETWORK ?= streaming_streaming

.PHONY: build test clean build-docker hello postgres-meta postgres-meta-docker

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
