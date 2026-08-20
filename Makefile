BINARY   := flowstitch
PKG      := github.com/kraicdesign/flow-stitch
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
REVISION ?= $(shell git rev-parse --verify HEAD 2>/dev/null || echo unknown)
CREATED  ?= $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
IMAGE    ?= kraicdesign/flowstitch
GOLANGCI_LINT_VERSION := 2.13.2
LDFLAGS  := -X main.version=$(VERSION)
GOFILES  := $(shell find . -name '*.go' -not -path './vendor/*')

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: validate
validate: fmt-check vet build test config-check ## Full gate: run this before every commit and in CI

.PHONY: build
build: ## Compile the binary into bin/
	@mkdir -p bin
	go build -ldflags '$(LDFLAGS)' -o bin/$(BINARY) ./cmd/flowstitch

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race ./...

.PHONY: test-short
test-short: ## Run unit tests without the race detector
	go test ./...

.PHONY: test-e2e
test-e2e: build ## Run real-process lifecycle and recovery tests
	FLOWSTITCH_E2E_BINARY='$(CURDIR)/bin/$(BINARY)' go test -tags=e2e -count=1 ./test/e2e

.PHONY: test-release
test-release: ## Test release preflight and mutation behaviour in scratch repositories
	./scripts/release-test.sh

.PHONY: cover
cover: ## Run tests and open a coverage report
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: fmt
fmt: ## Format all Go files
	gofmt -s -w $(GOFILES)

.PHONY: fmt-check
fmt-check: ## Fail if any file is not gofmt-clean
	@unformatted=$$(gofmt -s -l $(GOFILES)); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt-clean:"; echo "$$unformatted"; exit 1; \
	fi

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run the pinned golangci-lint version
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint v$(GOLANGCI_LINT_VERSION) is required; install it from https://golangci-lint.run/welcome/install/" >&2; \
		exit 1; \
	fi
	@if ! golangci-lint version | grep -q 'version $(GOLANGCI_LINT_VERSION) '; then \
		echo "golangci-lint v$(GOLANGCI_LINT_VERSION) is required; found: $$(golangci-lint version)" >&2; \
		exit 1; \
	fi
	golangci-lint run ./...

.PHONY: config-check
config-check: build ## Validate the example configuration before startup
	./bin/$(BINARY) -validate -config config/flowstitch.example.yaml

.PHONY: run
run: build ## Run with the example configuration
	./bin/$(BINARY) -config config/flowstitch.example.yaml

.PHONY: docker-build
docker-build: ## Build versioned and latest container images
	docker build --build-arg VERSION='$(VERSION)' \
		--build-arg REVISION='$(REVISION)' --build-arg CREATED='$(CREATED)' \
		-t '$(IMAGE):$(VERSION)' -t '$(IMAGE):latest' -t '$(IMAGE):$(REVISION)' .

.PHONY: docker-run
docker-run: ## Run the image with durable named state
	docker run --rm --name flowstitch -p 8080:8080 --stop-timeout 20 \
		--read-only --tmpfs /tmp:rw,noexec,nosuid,size=16m \
		--log-driver local --log-opt max-size=10m --log-opt max-file=3 \
		-v flowstitch-state:/var/lib/flowstitch \
		-v '$(CURDIR)/config/flowstitch.container.yaml:/etc/flowstitch/flowstitch.yaml:ro' \
		-e FLOWSTITCH_OPENSEARCH_USERNAME \
		-e FLOWSTITCH_OPENSEARCH_PASSWORD \
		'$(IMAGE):latest'

RELEASE_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

.PHONY: release
release: ## Verify, tag, and push a release (usage: make release X.Y.Z)
	./scripts/release.sh release $(foreach arg,$(RELEASE_ARGS),'$(arg)')

.PHONY: release-status
release-status: ## Inspect release readiness without mutation (usage: make release-status X.Y.Z)
	./scripts/release.sh status $(foreach arg,$(RELEASE_ARGS),'$(arg)')

ifneq ($(filter release release-status,$(firstword $(MAKECMDGOALS))),)
.PHONY: $(RELEASE_ARGS)
$(RELEASE_ARGS):
	@:
endif

.PHONY: compose-up
compose-up: ## Build and start the local OpenSearch demonstration stack
	FLOWSTITCH_VERSION='$(VERSION)' FLOWSTITCH_REVISION='$(REVISION)' \
		FLOWSTITCH_CREATED='$(CREATED)' \
		docker compose -f deploy/compose/docker-compose.yml up --build -d

.PHONY: compose-down
compose-down: ## Stop the demonstration stack without deleting its volumes
	docker compose -f deploy/compose/docker-compose.yml --profile input down

.PHONY: tidy
tidy: ## Tidy module requirements
	go mod tidy

.PHONY: clean
clean: ## Remove build output
	rm -rf bin coverage.out
