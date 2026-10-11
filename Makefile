GRADLEW := ./gradlew
PROJECT_VERSION := $(shell grep -E '^\s*version\s*=' build.gradle.kts | sed -E 's/.*"([^"]+)".*/\1/')
IMAGE_NAME := data-extraction

NAME ?= world

.PHONY: build test clean build-docker hello

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
