import { z } from 'zod'
import { conditionSchema, k8sMetadataSchema, workspaceIntentSchema } from './task'

const runtimePoolLifecycleSchema = z.enum([
  'Stopped',
  'Starting',
  'Serving',
  'Draining',
  'Quiescent',
  'Stopping',
  'Degraded',
  'Ambiguous',
])

const runtimePoolAdmissionSchema = z.enum(['Closed', 'Accepting', 'Draining', 'Ambiguous'])

const runtimeProfileSchema = z.object({
  protocolVersion: z.literal('orka.harness.v2').optional(),
  digest: z.string(),
  digestSchemaVersion: z.string(),
  acpProfile: z.string(),
  adapterDigests: z.record(z.string()),
  providerKind: z.string(),
  model: z.string(),
  modelLimits: z.object({ context: z.number(), output: z.number() }).optional(),
  agentConfigurationDigest: z.string(),
  toolPolicyDigest: z.string(),
  approvalPolicyDigest: z.string(),
  mcpConfigurationDigest: z.string(),
  workspaceIntent: workspaceIntentSchema,
  proxyCredentialRole: z.string(),
  proxyCredentialScope: z.string(),
  resourceClass: z.string(),
})

export const runtimePoolSchema = z.object({
  apiVersion: z.string().optional(),
  kind: z.string().optional(),
  metadata: k8sMetadataSchema,
  spec: z.object({
    trustDomain: z.object({ namespace: z.string(), identity: z.string() }),
    runtimeNamespace: z.string().optional(),
    runtime: z.object({
      image: z.string(),
      profile: runtimeProfileSchema,
    }),
    desiredReplicas: z.number().optional(),
    capacity: z.object({
      maxResidentSessions: z.number().optional(),
      maxRunningPrompts: z.number().optional(),
    }).optional(),
    coldStartTimeoutSeconds: z.number().optional(),
  }),
  status: z.object({
    observedGeneration: z.number().optional(),
    controllerEpoch: z.number().optional(),
    desiredReplicas: z.number().optional(),
    currentReplicas: z.number().optional(),
    lifecycle: runtimePoolLifecycleSchema.optional(),
    admissionState: runtimePoolAdmissionSchema.optional(),
    activeInstance: z.object({
      podNamespace: z.string(),
      podName: z.string(),
      podAddress: z.string(),
      podUID: z.string(),
      bootID: z.string(),
      runtimeInstanceID: z.string(),
      controllerEpoch: z.number(),
      protocolVersion: z.literal('orka.harness.v2'),
      profileDigest: z.string(),
      profileDigestSchemaVersion: z.string(),
      lastObservedTime: z.string().optional(),
    }).optional(),
    capacity: z.object({
      maxResidentSessions: z.number().optional(),
      maxRunningPrompts: z.number().optional(),
      residentSessions: z.number().optional(),
      runningPrompts: z.number().optional(),
      queuedTasks: z.number().optional(),
      reservedSessions: z.number().optional(),
      pendingPermissions: z.number().optional(),
      finalizingSessions: z.number().optional(),
      liveDescendants: z.number().optional(),
    }).optional(),
    message: z.string().optional(),
    conditions: z.array(conditionSchema).optional(),
  }).optional(),
})

const secretKeyRefSchema = z.object({
  name: z.string(),
  key: z.string(),
})

const agentRuntimeDeploymentSchema = z.object({
  mode: z.literal('external-endpoint'),
  endpoint: z.string(),
})

const agentRuntimeToolExecutionModeSchema = z.enum(['observed', 'brokered'])
const agentRuntimeBrokeredToolClassSchema = z.enum(['read', 'write', 'coordination'])

const agentRuntimeLimitsSchema = z.object({
  maxResidentSessions: z.number(),
  maxConcurrentPrompts: z.number(),
  maxRequestBytes: z.number(),
  maxEventLineBytes: z.number(),
  maxTerminalResultBytes: z.number(),
  maxBufferedEvents: z.number(),
  maxUpdateEventsPerSecond: z.number(),
  minPromptLeaseMillis: z.number(),
  maxPromptLeaseMillis: z.number(),
  maxPendingPermissions: z.number(),
  maxWorkspaceDeltaBytes: z.number(),
})

const agentRuntimeMCPPolicySchema = z.object({
  allowedTools: z.array(z.string()),
  disallowedTools: z.array(z.string()),
  allowBash: z.boolean(),
  approvalRequiredTools: z.array(z.string()),
})

const workspaceGovernanceSchema = z.object({
  mode: z.enum(['strict-governed', 'trusted-non-governed']),
  trusted: z.boolean(),
  orkaOwnedWorkspaceDeltas: z.boolean(),
  promptScopedBrokerAuthorization: z.boolean(),
  noDirectSCMPublication: z.boolean(),
  orkaOwnedCleanRoomPublication: z.boolean(),
  exactInstanceFencing: z.boolean(),
  duplicateSafeMutations: z.boolean(),
  cancellationSettlement: z.boolean(),
})

const agentRuntimeProfileSchema = z.object({
  digest: z.string(),
  digestSchemaVersion: z.number(),
  acpProfile: z.string(),
  adapterName: z.string(),
  adapterDigest: z.string(),
  providerKind: z.string(),
  model: z.string(),
  modelLimits: z.object({ context: z.number(), output: z.number() }).optional(),
  agentConfigurationDigest: z.string(),
  toolPolicyDigest: z.string(),
  approvalPolicyDigest: z.string(),
  mcpConfigurationDigest: z.string(),
  workspaceIntent: workspaceIntentSchema,
  proxyCredentialRole: z.string(),
  proxyCredentialScope: z.string(),
  resourceClass: z.string(),
})

const agentRuntimeV1CapabilitiesSchema = z.object({
  toolExecutionModes: z.array(agentRuntimeToolExecutionModeSchema).optional(),
  brokeredToolClasses: z.array(agentRuntimeBrokeredToolClassSchema).optional(),
  supportsCancel: z.boolean().optional(),
  supportsRuntimeSessions: z.boolean().optional(),
  supportsContinuation: z.boolean().optional(),
  supportsArtifacts: z.boolean().optional(),
}).strict()

const agentRuntimeV1SpecSchema = z.object({
  contractVersion: z.literal('orka.harness.v1'),
  deployment: agentRuntimeDeploymentSchema,
  clientAuth: z.object({
    bearerTokenSecretRef: secretKeyRefSchema,
  }).strict(),
  capabilities: agentRuntimeV1CapabilitiesSchema.optional(),
}).strict()

const agentRuntimeV2SpecSchema = z.object({
  contractVersion: z.literal('orka.harness.v2'),
  deployment: agentRuntimeDeploymentSchema,
  clientAuth: z.object({
    controllerBearerTokenSecretRef: secretKeyRefSchema,
    operationCapabilitySecretRef: secretKeyRefSchema,
  }).strict(),
  capabilities: z.object({
    runtimeInstanceID: z.string(),
    profile: agentRuntimeProfileSchema,
    // Stored v2 registrations created before MCP policy materialization may
    // omit this field. Read/list paths must preserve those records; dispatch
    // validates the policy before using the registration.
    mcpPolicy: agentRuntimeMCPPolicySchema.optional(),
    limits: agentRuntimeLimitsSchema,
    supportsDrain: z.boolean().default(false),
    supportsPublicationFinalization: z.boolean().optional(),
    workspaceGovernance: workspaceGovernanceSchema,
  }).strict(),
}).strict()

const agentRuntimeUnclassifiedSpecSchema = z.object({
  contractVersion: z.undefined().optional(),
  deployment: agentRuntimeDeploymentSchema,
  clientAuth: z.record(z.string(), z.unknown()),
  capabilities: z.record(z.string(), z.unknown()).optional(),
}).strict()

const agentRuntimeSpecSchema = z.union([
  agentRuntimeV1SpecSchema,
  agentRuntimeV2SpecSchema,
  agentRuntimeUnclassifiedSpecSchema,
])

const agentRuntimeObservedCapabilitiesSchema = z.object({
  protocolVersion: z.string().optional(),
  transport: z.string().optional(),
  acpVersion: z.string().optional(),
  runtimeInstanceID: z.string().optional(),
  supervisorBootID: z.string().optional(),
  controllerEpoch: z.number().optional(),
  runtimePoolUID: z.string().optional(),
  runtimePoolGeneration: z.number().optional(),
  runtimeProfileDigest: z.string().optional(),
  profileDigestSchemaVersion: z.number().optional(),
  adapterName: z.string().optional(),
  adapterDigest: z.string().optional(),
  providerKind: z.string().optional(),
  model: z.string().optional(),
  limits: agentRuntimeLimitsSchema.partial().optional(),
  supportsDrain: z.boolean().optional(),
  supportsPublicationFinalization: z.boolean().optional(),
  workspaceGovernance: workspaceGovernanceSchema.partial().optional(),
  lifecycle: z.string().optional(),
  runtimeName: z.string().optional(),
  runtimeVersion: z.string().optional(),
  toolExecutionModes: z.array(agentRuntimeToolExecutionModeSchema).optional(),
  brokeredToolClasses: z.array(agentRuntimeBrokeredToolClassSchema).optional(),
  supportsCancel: z.boolean().optional(),
  supportsRuntimeSessions: z.boolean().optional(),
  supportsContinuation: z.boolean().optional(),
  supportsArtifacts: z.boolean().optional(),
  supportsSuspend: z.boolean().optional(),
  supportsWorkspaceSnapshot: z.boolean().optional(),
  maxConcurrentTurns: z.number().optional(),
  maxTurnSeconds: z.number().optional(),
  maxOutputBytes: z.number().optional(),
})

export const agentRuntimeSchema = z.object({
  apiVersion: z.string().optional(),
  kind: z.string().optional(),
  metadata: k8sMetadataSchema,
  spec: agentRuntimeSpecSchema,
  status: z.object({
    ready: z.boolean().optional(),
    observedGeneration: z.number().optional(),
    observedCapabilities: agentRuntimeObservedCapabilitiesSchema.optional(),
    lastValidated: z.string().optional(),
    observedControllerAuthRefResourceVersion: z.string().optional(),
    observedOperationCapabilityRefResourceVersion: z.string().optional(),
    observedAuthRefResourceVersion: z.string().optional(),
    message: z.string().optional(),
    conditions: z.array(conditionSchema).optional(),
  }).optional(),
})

export const runtimePoolListSchema = z.object({
  items: z.array(runtimePoolSchema),
  metadata: z.object({ continue: z.string().optional(), remainingItemCount: z.number().optional() }).optional(),
})

export const agentRuntimeListSchema = z.object({
  items: z.array(agentRuntimeSchema),
  metadata: z.object({ continue: z.string().optional(), remainingItemCount: z.number().optional() }).optional(),
})

export type RuntimePool = z.infer<typeof runtimePoolSchema>
export type AgentRuntime = z.infer<typeof agentRuntimeSchema>
