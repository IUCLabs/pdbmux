BINARY  := pdbmux
DIST    := dist
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GOFLAGS := -ldflags="-s -w -X main.version=$(VERSION)"
OS      ?= $(shell go env GOOS)
ARCH    ?= $(shell go env GOARCH)

.PHONY: all build test e2e lint fmt clean install patch minor major

all: build

build:
	CGO_ENABLED=0 GOOS=$(OS) GOARCH=$(ARCH) go build $(GOFLAGS) -o $(DIST)/$(BINARY) .

test:
	go test -v -race ./...

# Needs a container runtime: two PostgreSQL, two openvoxdb and one Puppetboard.
e2e:
	TESTCONTAINERS_RYUK_DISABLED=true go test -tags e2e -race -count=1 -timeout=30m -v .

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .

clean:
	rm -rf $(DIST) $(BINARY)

install:
	go install $(GOFLAGS) .

DEFAULT_BRANCH ?= main

# Publish the release with generated notes, then push the tag; no workflow
# fills the release, so it is not drafted.
patch minor major:
	@git fetch --quiet --tags origin $(DEFAULT_BRANCH)
	@test "$$(git branch --show-current)" = "$(DEFAULT_BRANCH)" || { echo "ERROR: not on $(DEFAULT_BRANCH)"; exit 1; }
	@test -z "$$(git status --porcelain)" || { echo "ERROR: working tree not clean"; exit 1; }
	@test "$$(git rev-parse HEAD)" = "$$(git rev-parse origin/$(DEFAULT_BRANCH))" || { echo "ERROR: HEAD is not origin/$(DEFAULT_BRANCH)"; exit 1; }
	@LATEST=$$(git tag --list 'v*' --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$$' | head -1); \
	NEW=$$(echo "$${LATEST:-v0.0.0}" | sed 's/^v//' | awk -F. -v b=$@ '{ \
		if (b == "major") print "v" ($$1+1) ".0.0"; \
		else if (b == "minor") print "v" $$1 "." ($$2+1) ".0"; \
		else print "v" $$1 "." $$2 "." ($$3+1) }'); \
	SHA=$$(git rev-parse HEAD); \
	gh release create "$$NEW" --target "$$SHA" --generate-notes --title "$$NEW" || exit 1; \
	git push origin "$$SHA:refs/tags/$$NEW" || { gh release delete "$$NEW" --yes; exit 1; }
