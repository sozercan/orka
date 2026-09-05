# Current application release version. Chart.yaml has its own version and may
# advance independently for chart-only changes. Release preparation aligns both
# versions for a tagged application release.
VERSION := v0.1.1

# Image URL to use all building/pushing image targets
IMG ?= controller:latest
AI_WORKER_IMG ?= ghcr.io/orka-agents/orka/ai-worker:latest
GENERAL_WORKER_IMG ?= ghcr.io/orka-agents/orka/general-worker:latest
HARNESS_WRAPPER_IMG ?= ghcr.io/orka-agents/orka/agent-harness-wrapper:latest
ACP_CODEX_RUNTIME_IMG ?= ghcr.io/orka-agents/orka/acp-codex-runtime:latest
ACP_CLAUDE_RUNTIME_IMG ?= ghcr.io/orka-agents/orka/acp-claude-runtime:latest
ACP_COPILOT_RUNTIME_IMG ?= ghcr.io/orka-agents/orka/acp-copilot-runtime:latest
ACP_OPENCODE_RUNTIME_IMG ?= ghcr.io/orka-agents/orka/acp-opencode-runtime:latest
ACP_AGENTKIT_RUNTIME_IMG ?= ghcr.io/orka-agents/orka/acp-agentkit-runtime:latest
# AgentKit images contain the framework runtime plus one frozen
# /agent/agent.yaml. The Orka layer requires an immutable source image.
AGENTKIT_RUNTIME_IMAGE ?=
# Digest-pinned AgentKit source image identity used as the adapter authority.
AGENTKIT_ADAPTER_DIGEST ?=
WORKSPACE_PUBLISHER_IMG ?= ghcr.io/orka-agents/orka/workspace-publisher:latest
# Providers backing the generated docker-build-acp-<provider>-runtime and
# docker-push-acp-<provider>-runtime targets.
ACP_RUNTIME_PROVIDERS = codex claude copilot opencode
ACP_RUNTIME_IMGS = $(ACP_CODEX_RUNTIME_IMG) $(ACP_CLAUDE_RUNTIME_IMG) $(ACP_COPILOT_RUNTIME_IMG) $(ACP_OPENCODE_RUNTIME_IMG)
RUN_CONTROLLER_MODE ?= harness-v2
RUN_WATCH_NAMESPACE ?= orka-system
RUN_AGENT_EXECUTION_SNAPSHOT_KEY_FILE ?=
RUN_EXECUTION_MODE_CONTROLLER_USERNAMES ?= $(shell "$(KUBECTL)" auth whoami -o jsonpath='{.status.userInfo.username}' 2>/dev/null)

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen kustomize ## Generate canonical and staged manifests.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd:allowDangerousTypes=true webhook paths="./..." output:crd:artifacts:config=config/crd/bases
	@set -euo pipefail; \
		tmp="$$(mktemp -d .manifest_staging.tmp.XXXXXX)"; \
		backup=""; \
		cleanup() { \
			rc=$$?; \
			trap - EXIT; \
			[[ -z "$$tmp" ]] || rm -rf "$$tmp"; \
			if [[ -n "$$backup" && -e "$$backup" && ! -e manifest_staging ]]; then mv "$$backup" manifest_staging; fi; \
			exit $$rc; \
		}; \
		trap cleanup EXIT; \
		mkdir -p "$$tmp/deploy" "$$tmp/charts/orka"; \
		"$(KUSTOMIZE)" build config/acp-production -o "$$tmp/deploy/orka.yaml"; \
		"$(KUSTOMIZE)" build \
			--load-restrictor LoadRestrictionsNone \
			cmd/build/helmify | go run ./cmd/build/helmify -output-dir "$$tmp/charts/orka"; \
		if [[ -e manifest_staging ]]; then \
			backup="$$(mktemp -d .manifest_staging.backup.XXXXXX)"; \
			rmdir "$$backup"; \
			mv manifest_staging "$$backup"; \
		fi; \
		mv "$$tmp" manifest_staging; \
		tmp=""; \
		trap - EXIT; \
		if [[ -n "$$backup" ]]; then rm -rf "$$backup"; fi

.PHONY: release-manifest
release-manifest: ## Prepare staging manifests for NEWVERSION=vX.Y.Z[-beta.N|-rc.N].
	@test -n "$(NEWVERSION)" || { echo "NEWVERSION is required" >&2; exit 2; }
	python3 scripts/update-release-version.py "$(NEWVERSION)"
	$(MAKE) manifests

.PHONY: promote-staging-manifest
promote-staging-manifest: ## Promote committed staging manifests into release snapshots.
	test -f manifest_staging/deploy/orka.yaml
	test -f manifest_staging/charts/orka/Chart.yaml
	@set -euo pipefail; \
		stage="$$(mktemp -d .promote-staging.tmp.XXXXXX)"; \
		backup="$$(mktemp -d .promote-staging.backup.XXXXXX)"; \
		installed_deploy=0; \
		installed_charts=0; \
		rollback() { \
			rc=$$?; \
			trap - EXIT; \
			if [[ $$installed_deploy -eq 1 ]]; then rm -rf deploy; fi; \
			if [[ $$installed_charts -eq 1 ]]; then rm -rf charts; fi; \
			if [[ -e "$$backup/deploy" ]]; then mv "$$backup/deploy" deploy; fi; \
			if [[ -e "$$backup/charts" ]]; then mv "$$backup/charts" charts; fi; \
			rm -rf "$$stage" "$$backup"; \
			exit $$rc; \
		}; \
		trap rollback EXIT; \
		cp -R manifest_staging/deploy "$$stage/deploy"; \
		cp -R manifest_staging/charts "$$stage/charts"; \
		if [[ -e deploy ]]; then mv deploy "$$backup/deploy"; fi; \
		if [[ -e charts ]]; then mv charts "$$backup/charts"; fi; \
		mv "$$stage/deploy" deploy; installed_deploy=1; \
		mv "$$stage/charts" charts; installed_charts=1; \
		trap - EXIT; \
		rm -rf "$$stage" "$$backup"
	diff --no-dereference --recursive manifest_staging/deploy deploy
	diff --no-dereference --recursive manifest_staging/charts/orka charts/orka

.PHONY: verify-release-manifest
verify-release-manifest: ## Validate promoted release snapshots and the harness-v2 Helm render contract.
	scripts/validate-release-manifest.sh "$(if $(NEWVERSION),$(NEWVERSION),$(VERSION))"

.PHONY: sync-helm-crds
sync-helm-crds: ## Synchronize generated CRDs into the promoted Helm chart while preserving non-CRD files.
	scripts/sync-helm-crds.sh

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: ensure-ui-embed
ensure-ui-embed: ## Create stub UI embed directory if not present (for go vet/build without full UI build).
	@if [ ! -d internal/uiembed/dist ]; then \
		mkdir -p internal/uiembed/dist && \
		echo '<!doctype html><html><body>stub</body></html>' > internal/uiembed/dist/index.html; \
	fi

.PHONY: vet
vet: ensure-ui-embed ## Run go vet against code.
	go vet ./...


.PHONY: repository-monitor-fake-e2e
repository-monitor-fake-e2e: ensure-ui-embed ## Run fake-GitHub RepositoryMonitor issue-to-PR E2E scenarios
	bash scripts/repository-monitor-fake-e2e.sh

.PHONY: repository-monitor-validate
repository-monitor-validate: ensure-ui-embed ## Run full local RepositoryMonitor fake-E2E/docs/example validation
	bash scripts/repository-monitor-validate.sh

.PHONY: repository-monitor-live-preflight
repository-monitor-live-preflight: ## Check prerequisites for live GitHub label trigger E2E without changing the cluster
	bash scripts/live-github-label-trigger-e2e.sh --preflight-only

.PHONY: repository-monitor-completion-audit
repository-monitor-completion-audit: ensure-ui-embed ## Run local validation plus live preflight audit for RepositoryMonitor plan completion
	bash scripts/repository-monitor-completion-audit.sh

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# The default e2e setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
KIND_CLUSTER ?= orka-test-e2e
E2E_GO_TEST_TIMEOUT ?= 50m
E2E_GINKGO_FOCUS ?=
E2E_GINKGO_FOCUS_ARG = $(if $(E2E_GINKGO_FOCUS),-ginkgo.focus="$(E2E_GINKGO_FOCUS)",)

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) --config test/e2e/kind-config.yaml ;; \
	esac

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) go test -tags=e2e ./test/e2e/ -timeout $(E2E_GO_TEST_TIMEOUT) -v -ginkgo.v $(E2E_GINKGO_FOCUS_ARG)
	$(MAKE) cleanup-test-e2e

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: test-e2e-setup-only
test-e2e-setup-only: setup-test-e2e docker-build-all ## Set up Kind cluster and build all images without running tests.
	@echo "Loading images into Kind cluster '$(KIND_CLUSTER)'..."
	$(KIND) load docker-image $(IMG) --name $(KIND_CLUSTER)
	$(KIND) load docker-image $(AI_WORKER_IMG) --name $(KIND_CLUSTER)
	$(KIND) load docker-image $(GENERAL_WORKER_IMG) --name $(KIND_CLUSTER)
	$(KIND) load docker-image $(HARNESS_WRAPPER_IMG) --name $(KIND_CLUSTER)
	set -e; for img in $(ACP_RUNTIME_IMGS); do $(KIND) load docker-image $$img --name $(KIND_CLUSTER); done
	$(KIND) load docker-image $(WORKSPACE_PUBLISHER_IMG) --name $(KIND_CLUSTER)

.PHONY: lint
lint: ensure-ui-embed golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run --new

.PHONY: lint-fix
lint-fix: ensure-ui-embed golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix --new

##@ Demos

.PHONY: demo-cluster-up
demo-cluster-up: ## Bootstrap a kind cluster with Orka + agent-sandbox
	hack/demos/cluster/cluster-up.sh
	hack/demos/cluster/install-agent-sandbox.sh
	hack/demos/cluster/install-demo-model.sh

.PHONY: demo-cluster-down
demo-cluster-down: ## Tear down the kind demo cluster
	hack/demos/cluster/cluster-down.sh

.PHONY: demo-substrate-up
demo-substrate-up: ## Bootstrap a DEDICATED kind cluster with Agent Substrate + Orka (Demo 70)
	hack/demos/cluster/install-substrate.sh

.PHONY: demo-substrate-down
demo-substrate-down: ## Tear down the Agent Substrate demo cluster (Demo 70)
	kind delete cluster --name $${KIND_CLUSTER:-orka-agent-substrate-e2e}

.PHONY: demo-cluster-up-all
demo-cluster-up-all: ## ONE substrate-flavored kind cluster that runs the local demos (00-40, 60-70)
	hack/demos/cluster/install-substrate.sh
	ORKA_DEMO_CLUSTER=$${KIND_CLUSTER:-orka-agent-substrate-e2e} hack/demos/cluster/install-demo-model.sh
	ORKA_DEMO_CLUSTER=$${KIND_CLUSTER:-orka-agent-substrate-e2e} hack/demos/cluster/install-agent-sandbox.sh

.PHONY: demo-cluster-up-all-down
demo-cluster-up-all-down: demo-substrate-down ## Tear down the unified demo cluster (alias of demo-substrate-down)

.PHONY: demo-images
demo-images: ## Build + kind-load demo-only sandbox runtime image
	docker build -t orka-sandbox-runtime:demo -f hack/demos/images/sandbox-runtime/Dockerfile .
	kind load docker-image orka-sandbox-runtime:demo --name $${ORKA_DEMO_CLUSTER:-orka-demo}

.PHONY: demo-test
demo-test: ## Run hack/demos smoke tests (style helpers, profile dispatch, payoff cards)
	bash hack/demos/lib/test/run-all.sh

##@ UI

.PHONY: ui-install
ui-install: ## Install UI dependencies.
	cd ui && bun install

.PHONY: ui-dev
ui-dev: ## Run UI dev server.
	cd ui && bun run dev

.PHONY: ui-build
ui-build: ui-install ## Build UI and copy to embed directory.
	cd ui && bun run build
	rm -rf internal/uiembed/dist
	cp -r ui/dist internal/uiembed/dist

.PHONY: ui-lint
ui-lint: ## Run UI linter.
	cd ui && bun run lint

.PHONY: ui-test
ui-test: ## Run UI unit tests.
	cd ui && bun run test

.PHONY: ui-test-coverage
ui-test-coverage: ## Run UI unit tests with coverage.
	cd ui && bun run test:coverage

##@ Build

.PHONY: build
build: manifests generate fmt vet ui-build ## Build manager and admission binaries.
	go build -o bin/manager ./cmd
	go build -o bin/orka-admission ./cmd/orka-admission

.PHONY: docs-cli
docs-cli: build-cli ## Generate CLI command reference docs.
	scripts/generate-cli-docs.sh

.PHONY: docs-cli-check
docs-cli-check: build-cli ## Check generated CLI command reference docs are up to date.
	scripts/generate-cli-docs.sh --check

.PHONY: build-cli
build-cli: ## Build orka CLI binary.
	go build -ldflags "-X main.version=$(shell git describe --tags --always --dirty 2>/dev/null || echo dev)" -o bin/orka ./cmd/cli/

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	POD_NAMESPACE="$(RUN_WATCH_NAMESPACE)" go run ./cmd --leader-elect=true \
		--controller-mode="$(RUN_CONTROLLER_MODE)" \
		--watch-namespace="$(RUN_WATCH_NAMESPACE)" \
		--agent-execution-snapshot-key-file="$(RUN_AGENT_EXECUTION_SNAPSHOT_KEY_FILE)" \
		--enforce-namespace-isolation=true \
		--execution-mode-controller-usernames="$(RUN_EXECUTION_MODE_CONTROLLER_USERNAMES)"

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

.PHONY: docker-build-ai-worker
docker-build-ai-worker: ## Build docker image for the AI worker.
	$(CONTAINER_TOOL) build -t ${AI_WORKER_IMG} -f workers/ai/Dockerfile .

.PHONY: docker-build-general-worker
docker-build-general-worker: ## Build docker image for the general worker.
	$(CONTAINER_TOOL) build -t ${GENERAL_WORKER_IMG} -f workers/general/Dockerfile .

.PHONY: docker-build-harness-wrapper
docker-build-harness-wrapper: ## Build the opt-in harness v1 compatibility wrapper image.
	$(CONTAINER_TOOL) build -t ${HARNESS_WRAPPER_IMG} -f workers/harness/Dockerfile .

# Recipes for the docker-build-acp-<provider>-runtime targets are generated
# from ACP_RUNTIME_PROVIDERS below; these dependency-less rules carry their
# `make help` entries.
docker-build-acp-codex-runtime: ## Build the immutable Codex ACP runtime image.
docker-build-acp-claude-runtime: ## Build the immutable Claude ACP runtime image.
docker-build-acp-copilot-runtime: ## Build the immutable GitHub Copilot ACP runtime image.
docker-build-acp-opencode-runtime: ## Build the immutable OpenCode ACP runtime image.

.PHONY: docker-build-acp-agentkit-runtime
docker-build-acp-agentkit-runtime: ## Layer the Orka supervisor onto a digest-pinned AgentKit runtime image.
	$(CONTAINER_TOOL) build \
		--build-arg AGENTKIT_RUNTIME_IMAGE="$(AGENTKIT_RUNTIME_IMAGE)" \
		--build-arg AGENTKIT_ADAPTER_DIGEST="$(AGENTKIT_ADAPTER_DIGEST)" \
		-t ${ACP_AGENTKIT_RUNTIME_IMG} \
		-f workers/acp/images/agentkit/Dockerfile .

.PHONY: docker-build-workspace-publisher
docker-build-workspace-publisher: ## Build the clean-room workspace publisher image.
	$(CONTAINER_TOOL) build -t ${WORKSPACE_PUBLISHER_IMG} -f workers/publisher/Dockerfile .

.PHONY: docker-push-ai-worker
docker-push-ai-worker: ## Push docker image for the AI worker.
	$(CONTAINER_TOOL) push ${AI_WORKER_IMG}

.PHONY: docker-push-general-worker
docker-push-general-worker: ## Push docker image for the general worker.
	$(CONTAINER_TOOL) push ${GENERAL_WORKER_IMG}

.PHONY: docker-push-harness-wrapper
docker-push-harness-wrapper: ## Push the opt-in harness v1 compatibility wrapper image.
	$(CONTAINER_TOOL) push ${HARNESS_WRAPPER_IMG}

# Recipes for the docker-push-acp-<provider>-runtime targets are generated
# from ACP_RUNTIME_PROVIDERS below; these dependency-less rules carry their
# `make help` entries.
docker-push-acp-codex-runtime: ## Push the immutable Codex ACP runtime image.
docker-push-acp-claude-runtime: ## Push the immutable Claude ACP runtime image.
docker-push-acp-copilot-runtime: ## Push the immutable GitHub Copilot ACP runtime image.
docker-push-acp-opencode-runtime: ## Push the immutable OpenCode ACP runtime image.

.PHONY: docker-push-acp-agentkit-runtime
docker-push-acp-agentkit-runtime: ## Push an AgentKit ACP runtime image built from a frozen agent image.
	$(CONTAINER_TOOL) push ${ACP_AGENTKIT_RUNTIME_IMG}

# acp-provider-uc maps an ACP runtime provider word to the uppercase form used
# in its image variable name (ACP_<PROVIDER>_RUNTIME_IMG).
acp-provider-uc = $(subst codex,CODEX,$(subst claude,CLAUDE,$(subst copilot,COPILOT,$(subst opencode,OPENCODE,$(1)))))

# acp-runtime-image-targets generates the build/push recipes for one ACP
# runtime provider word $(1) (target names, Dockerfile directory, and image
# variable).
define acp-runtime-image-targets
.PHONY: docker-build-acp-$(1)-runtime docker-push-acp-$(1)-runtime
docker-build-acp-$(1)-runtime:
	$$(CONTAINER_TOOL) build -t $${ACP_$(call acp-provider-uc,$(1))_RUNTIME_IMG} -f workers/acp/images/$(1)/Dockerfile .
docker-push-acp-$(1)-runtime:
	$$(CONTAINER_TOOL) push $${ACP_$(call acp-provider-uc,$(1))_RUNTIME_IMG}
endef

$(foreach provider,$(ACP_RUNTIME_PROVIDERS),$(eval $(call acp-runtime-image-targets,$(provider))))

.PHONY: docker-push-workspace-publisher
docker-push-workspace-publisher: ## Push the clean-room workspace publisher image.
	$(CONTAINER_TOOL) push ${WORKSPACE_PUBLISHER_IMG}

.PHONY: docker-build-all
docker-build-all: docker-build docker-build-ai-worker docker-build-general-worker docker-build-harness-wrapper docker-build-acp-codex-runtime docker-build-acp-claude-runtime docker-build-acp-copilot-runtime docker-build-acp-opencode-runtime docker-build-workspace-publisher ## Build all docker images.

.PHONY: docker-push-all
docker-push-all: docker-push docker-push-ai-worker docker-push-general-worker docker-push-harness-wrapper docker-push-acp-codex-runtime docker-push-acp-claude-runtime docker-push-acp-copilot-runtime docker-push-acp-opencode-runtime docker-push-workspace-publisher ## Push all docker images.

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: verify-acp-runtime-images
verify-acp-runtime-images: ## Require digest-pinned ACP runtime images for supported deployments.
	@for entry in \
		"IMG=$(IMG)" \
		"WORKSPACE_PUBLISHER_IMG=$(WORKSPACE_PUBLISHER_IMG)" \
		"ACP_CODEX_RUNTIME_IMG=$(ACP_CODEX_RUNTIME_IMG)" \
		"ACP_CLAUDE_RUNTIME_IMG=$(ACP_CLAUDE_RUNTIME_IMG)" \
		"ACP_COPILOT_RUNTIME_IMG=$(ACP_COPILOT_RUNTIME_IMG)" \
		"ACP_OPENCODE_RUNTIME_IMG=$(ACP_OPENCODE_RUNTIME_IMG)"; do \
		name="$${entry%%=*}"; ref="$${entry#*=}"; \
		if [[ ! "$${ref}" =~ ^.+@sha256:[0-9a-f]{64}$$ ]]; then \
			echo "$$name must be an immutable image reference ending in @sha256:<64 lowercase hex characters>; got '$$ref'" >&2; \
			exit 1; \
		fi; \
	done

.PHONY: verify-static-mode-crds
verify-static-mode-crds: ## Refuse workload deployment until the platform-owned shared CRD bundle is ready.
	@for crd in \
		agentruntimes.core.orka.ai \
		agents.core.orka.ai \
		branchclaims.core.orka.ai \
		controllerepochs.core.orka.ai \
		executionworkspaceclasses.workspace.orka.ai \
		executionworkspacepools.workspace.orka.ai \
		executionworkspaceproviders.workspace.orka.ai \
		executionworkspaces.workspace.orka.ai \
		externaleffects.core.orka.ai \
		gatewaybindings.gateway.orka.ai \
		gatewayclasses.gateway.orka.ai \
		gateways.gateway.orka.ai \
		outboundaccesspolicies.core.orka.ai \
		promptattempts.core.orka.ai \
		providers.core.orka.ai \
		publications.core.orka.ai \
		repositorymonitors.core.orka.ai \
		repositoryscans.core.orka.ai \
		runtimepools.core.orka.ai \
		runtimesessioncontrols.core.orka.ai \
		skills.core.orka.ai \
		substrateactorpools.core.orka.ai \
		tasks.core.orka.ai \
		tools.core.orka.ai; do \
		"$(KUBECTL)" get crd "$$crd" >/dev/null || { echo "missing shared CRD: $$crd; apply the platform-owned static-mode CRD wave before workloads" >&2; exit 1; }; \
		"$(KUBECTL)" wait --for=condition=Established --timeout=60s "crd/$$crd" >/dev/null || { echo "shared CRD is not Established: $$crd" >&2; exit 1; }; \
	done
	@for crd in agentexecutioncontrols.core.orka.ai agentexecutionpolicies.core.orka.ai agentexecutionadjudications.core.orka.ai; do \
		if "$(KUBECTL)" get crd "$$crd" >/dev/null 2>&1; then \
			echo "unsupported superseded coexistence CRD remains installed: $$crd" >&2; \
			exit 1; \
		fi; \
	done
	@"$(KUBECTL)" get crd agentruntimes.core.orka.ai -o json | jq -e \
		'[.spec.versions[] | select(.served == true) | .schema.openAPIV3Schema.properties.spec.properties.contractVersion.enum] as $$enums | ($$enums | length) > 0 and ($$enums | all(sort == ["orka.harness.v1","orka.harness.v2"]))' >/dev/null || \
		{ echo "AgentRuntime CRD is not the shared orka.harness.v1/orka.harness.v2 schema; apply the platform-owned static-mode CRD wave before workloads" >&2; exit 1; }
	@"$(KUBECTL)" get crd agents.core.orka.ai -o json | jq -e \
		'[.spec.versions[] | select(.served == true) | .schema.openAPIV3Schema.properties.spec.properties.runtime as $$runtime | (((($$runtime.properties.contractVersion.enum // []) | sort) == ["orka.harness.v1","orka.harness.v2"]) and ((($$runtime["x-kubernetes-validations"] // []) | map(.message) | index("runtime.contractVersion is immutable once set")) != null))] as $$checks | ($$checks | length) > 0 and ($$checks | all)' >/dev/null || \
		{ echo "Agent CRD is missing the immutable shared contract selector; apply the platform-owned static-mode CRD wave before workloads" >&2; exit 1; }
	@"$(KUBECTL)" get crd tasks.core.orka.ai -o json | jq -e \
		'[.spec.versions[] | select(.served == true) | .schema.openAPIV3Schema as $$schema | $$schema.properties.status as $$status | ((($$status.properties.agentExecutionBinding.type // "") == "object") and ((($$status.properties.agentExecutionBinding.properties.contractVersion.enum // []) | sort) == ["orka.harness.v1","orka.harness.v2"]) and (($$status.properties | has("agentExecutionNoExecution")) | not) and (($$status.properties | has("agentExecutionQuarantine")) | not) and (($$status.properties | has("agentExecutionResolutionRef")) | not) and ((($$status["x-kubernetes-validations"] // []) | map(.message) | index("agentExecutionBinding is write-once and immutable")) != null) and ((($$schema["x-kubernetes-validations"] // []) | map(.message) | index("Task spec is immutable after execution authority is recorded")) != null))] as $$checks | ($$checks | length) > 0 and ($$checks | all)' >/dev/null || \
		{ echo "Task CRD is missing the static-mode execution-authority schema; apply the platform-owned static-mode CRD wave before workloads" >&2; exit 1; }

.PHONY: deploy
deploy: verify-acp-runtime-images verify-static-mode-crds manifests kustomize ## Deploy the static harness-v2 installation after the shared CRD wave.
	@bash "$(CURDIR)/scripts/lib/ensure-static-mode-namespace.sh" "$(KUBECTL)" orka-system harness-v2
	@if ! "$(KUBECTL)" -n orka-system get secret acp-artifact-capability >/dev/null 2>&1; then \
		secret="$$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | base64 | tr -d '\n')"; \
		"$(KUBECTL)" -n orka-system create secret generic acp-artifact-capability --from-literal=capability-secret="$$secret"; \
	fi
	@if ! "$(KUBECTL)" -n orka-system get secret workspace-publisher-auth >/dev/null 2>&1; then \
		bearer="$$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | base64 | tr -d '\n')"; \
		capability="$$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | base64 | tr -d '\n')"; \
		"$(KUBECTL)" -n orka-system create secret generic workspace-publisher-auth --from-literal=controller-token="$$bearer" --from-literal=operation-capability-secret="$$capability"; \
	fi
	@if ! "$(KUBECTL)" -n orka-system get secret provider-auth-proxy >/dev/null 2>&1; then \
		token="$$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | base64 | tr -d '\n')"; \
		"$(KUBECTL)" -n orka-system create secret generic provider-auth-proxy --from-literal=token="$$token"; \
	fi
	@if ! "$(KUBECTL)" -n orka-system get secret scm-egress-proxy-auth >/dev/null 2>&1; then \
		token="$$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n')"; \
		"$(KUBECTL)" -n orka-system create secret generic scm-egress-proxy-auth --from-literal=token="$$token"; \
	fi
	@set -eu; tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
		cp -R config "$$tmp/config"; \
		"$(CURDIR)/scripts/render-worker-images.sh" "$$tmp/config/manager/manager.yaml" \
			"$(AI_WORKER_IMG)" "$(GENERAL_WORKER_IMG)"; \
		"$(CURDIR)/scripts/render-acp-runtime-images.sh" "$$tmp/config/acp-production" \
			"${ACP_CODEX_RUNTIME_IMG}" "${ACP_CLAUDE_RUNTIME_IMG}" "${ACP_COPILOT_RUNTIME_IMG}" "${ACP_OPENCODE_RUNTIME_IMG}"; \
		cd "$$tmp/config/acp-production"; \
		"$(KUSTOMIZE)" edit set image \
			controller=${IMG} \
			ghcr.io/orka-agents/orka=${IMG} \
			docker.io/sozercan/orka-workspace-publisher=${WORKSPACE_PUBLISHER_IMG}; \
		"$(CURDIR)/scripts/apply-acp-production.sh" "$$PWD" "$(KUSTOMIZE)" "$(KUBECTL)"


.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUBECTL)" delete validatingwebhookconfiguration orka-admission --ignore-not-found=true
	"$(KUSTOMIZE)" build config/acp-production | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.7.1
CONTROLLER_TOOLS_VERSION ?= v0.20.0

#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?([0-9]+)\.([0-9]+).*/release-\1.\2/')

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.13.1
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
