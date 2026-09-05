---
slug: /upgrading
description: "Upgrading Orka, including the CRD step Helm will not do for you."
---

# Upgrading

Orka is pre-1.0 and its CRDs still change between releases. Upgrades are safe but not
automatic — there is one manual step Helm cannot do for you.

## The one thing that will bite you

**Helm never creates or updates CRDs during `helm upgrade`.** Files in a chart's `crds/`
directory are applied on install and ignored on every upgrade after that.

That is standard Helm behavior, not something Orka chose or can switch off. The result if
you skip the step: the controller runs new code against old CRD schemas, and any field the
new version added is silently dropped by the API server. Resources look accepted and do
nothing.

So: **apply the CRDs from the target chart yourself, before every upgrade** — including
when upgrading from a release that installed no CRDs at all.

## Procedure

Use a host with Bash, Helm, kubectl, and jq installed. Choose the target chart and
Kubernetes context before taking backups:

```bash
export TARGET_CHART='<path-or-reference-to-target-chart>'
export TARGET_CONTEXT='<kubeconfig-context>'
```

Use that context for the backup, CRD update, upgrade, and verification commands below.

### 1. Back up first

Back up both Kubernetes state and the controller's volume before upgrading:

- The controller's **SQLite store**, on its PersistentVolumeClaim. This holds transcripts,
  gateway delivery records, and artifact payloads.
- **Kubernetes objects**, including operator configuration and the controller-owned ACP
  execution, fencing, publication, and idempotency records.

The JSON exports below provide additional records for inspection and configuration
reference. They complement your cluster's backup system.

```bash
kubectl --context "$TARGET_CONTEXT" -n orka-system get \
  agents,providers,tools,skills,tasks,repositorymonitors,repositoryscans,\
outboundaccesspolicies,gateways,gatewaybindings,agentruntimes,substrateactorpools \
  -o json > orka-crs.json

# Cluster-scoped, so no -n:
kubectl --context "$TARGET_CONTEXT" get gatewayclasses -o json > orka-gatewayclasses.json
```

For an ACP install, also export the controller-owned control state:

```bash
kubectl --context "$TARGET_CONTEXT" -n orka-system get \
  runtimepools,controllerepochs,promptattempts,runtimesessioncontrols,\
publications,externaleffects \
  -o json > orka-acp-control-state.json

# BranchClaims are cluster-scoped:
kubectl --context "$TARGET_CONTEXT" get branchclaims -o json > orka-branchclaims.json
```

:::warning[Exports are not an ACP recovery procedure]
These JSON files alone do not safely restore in-flight execution or replay protection.
Do not use `kubectl apply` on controller-owned exports to reconstruct lost ACP authority.
A tested procedure for restoring Kubernetes state together with the controller volume
is follow-up work; this upgrade procedure keeps existing control resources in place.
:::

If you have the workspace provider API enabled, back up its resources too:

```bash
kubectl --context "$TARGET_CONTEXT" -n orka-system get \
  executionworkspaceclasses,executionworkspaceproviders,executionworkspacepools,\
executionworkspaces,runtimeproviderconfigs,runtimeworkspaceprofiles \
  -o json > orka-workspace-crs.json
```

Do not stop at the classes. A class is unusable without the
`RuntimeProviderConfig` and `RuntimeWorkspaceProfile` objects its `parametersRef` points
at, so a backup holding only classes, providers, and pools restores a set of resources
that cannot run anything.

Leave out any kind your cluster does not have; `kubectl` fails the whole command on an
unknown resource rather than skipping it.

:::danger[Do not copy the SQLite file from a running controller]
Copying `orka.db` while the controller is writing produces a backup that restores without
error and is missing records. Snapshot the whole PVC, or stop the controller first.
[Gateways](gateways.md) explains why this one matters more than it looks.
:::

### 2. Apply the target CRDs

From a checkout matching the version you are upgrading to:

```bash
scripts/apply-helm-crds.sh "$TARGET_CHART" "$TARGET_CONTEXT"
```

That patches each CRD with an optimistic-concurrency check and waits for `Established`.
The equivalent by hand is in the
[chart README](https://github.com/orka-agents/orka/blob/main/manifest_staging/charts/orka/README.md).

If a separate platform team or GitOps system owns CRDs in your cluster, do this step
through that system instead, wait for every Orka CRD to become `Established`, then
continue. Do not run two CRD apply workflows against one cluster.

### 3. Upgrade

```bash
helm upgrade orka "$TARGET_CHART" --kube-context "$TARGET_CONTEXT" \
  --namespace orka-system --wait --timeout 10m
```

Keep the timeout longer than the controller's termination grace period plus time for
the replacement Pod to become Ready. The harness-v2 default grace period is six minutes;
increase the timeout if you configure a longer drain or need more rollout time.

### 4. Verify

```bash
# A Helm release named `orka` creates a Deployment named `orka-controller`.
kubectl --context "$TARGET_CONTEXT" -n orka-system rollout status deploy/orka-controller
kubectl --context "$TARGET_CONTEXT" get crd -o name | grep '\.orka\.ai$' | wc -l
```

The CRD count should match the target chart. Orka CRDs are the ones whose group ends in
`.orka.ai`; they carry no common label, so counting by name is the check that actually
works. [Release status](../reference/release-status.md) lists the count per version.

For targets that include `runtimepools.core.orka.ai`, also check the runtime pools.
Skip this check for v0.1.3, which has no RuntimePool CRD:

```bash
kubectl --context "$TARGET_CONTEXT" -n orka-system get runtimepools
```

Submit one small Task and confirm it reaches `Succeeded`.

## Values you cannot change on upgrade

The chart blocks these, because changing them would orphan data or split a control plane
in two:

| Value | Why it is fixed |
| --- | --- |
| `controller.mode` | The execution contract is the installation's identity. |
| `controller.watchNamespace` | Existing Tasks live there. |
| `controller.agentExecutionSnapshot.existingSecret` and `.key` | Retained snapshots become undecryptable. |
| `controller.acpRuntime.namespace` | Running pools live there. |
| The release fullname | Every owned resource is named from it. |

To change one, install a new release alongside the old one and migrate producers across.
See [Harness modes](harness-modes.md).

## `--skip-crds`

Use `--skip-crds` **only** when one designated owner already manages Orka's CRDs for the
cluster — a platform team, a GitOps controller, or a previous release whose CRDs were
retained after uninstall. Every other install should let Helm create them.

If you uninstalled a previous release, update its retained CRDs first, then install the
replacement with `--skip-crds`.

## Uninstall

```bash
helm uninstall orka --kube-context "$TARGET_CONTEXT" --namespace orka-system
```

That removes the release's resources and **keeps** the CRDs and every custom resource
stored under them — again, standard Helm `crds/` behavior, not a chart value.

:::danger[Deleting a CRD deletes its data]
Removing an Orka CRD deletes every custom resource of that kind across the cluster, with no
undo. Treat it as a deliberate cluster-wide data destruction step, performed only after the
resources are gone or backed up.
:::

## Migrating harness v1 to v2

This is not an upgrade. The two contracts run as separate installations, and Tasks do not
move between them. The procedure — stand v2 up, point producers at it, drain v1 — is in
[Harness modes](harness-modes.md).

For clusters still holding `orka.harness.v1` AgentRuntimes, `scripts/upgrade-orka-crds.sh`
performs the one-way cutover. It refuses to run while any v1 AgentRuntime, dependent Agent,
affected GatewayBinding, Task using the removed `gitSecretRef` fields, or legacy wrapper
workload remains, and it requires attested backups of both the store and the custom
resources before it will apply anything.
