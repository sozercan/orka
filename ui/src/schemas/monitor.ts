import { z } from 'zod'
import { agentRefSchema, k8sMetadataSchema } from './task'

const repositoryMonitorTargetSchema = z.object({
  enabled: z.boolean().optional(),
  includeDrafts: z.boolean().optional(),
  maxPerRun: z.number().optional(),
})

const repositoryMonitorIssueTargetSchema = z.object({
  enabled: z.boolean().optional(),
  maxPerRun: z.number().optional(),
  includeLabels: z.array(z.string()).optional(),
  excludeLabels: z.array(z.string()).optional(),
})

const repositoryMonitorIssueCommandLabelsSchema = z.object({
  triage: z.string().optional(),
  research: z.string().optional(),
  plan: z.string().optional(),
  approvePlan: z.string().optional(),
  implement: z.string().optional(),
  decompose: z.string().optional(),
  stop: z.string().optional(),
  resume: z.string().optional(),
})

const repositoryMonitorPullRequestCommandLabelsSchema = z.object({
  review: z.string().optional(),
  fix: z.string().optional(),
  fixCI: z.string().optional(),
  updateBranch: z.string().optional(),
  automerge: z.string().optional(),
  stop: z.string().optional(),
  resume: z.string().optional(),
})

const repositoryMonitorTriggersSchema = z.object({
  github: z.object({
    labels: z.object({
      enabled: z.boolean().optional(),
      consumeCommandLabels: z.boolean().optional(),
      requireActorPermission: z.enum(['write', 'maintain', 'admin']).optional(),
      issues: repositoryMonitorIssueCommandLabelsSchema.optional(),
      pullRequests: repositoryMonitorPullRequestCommandLabelsSchema.optional(),
    }).optional(),
  }).optional(),
})

export const repositoryMonitorSpecSchema = z.object({
  provider: z.string().optional(),
  repoURL: z.string(),
  owner: z.string().optional(),
  repository: z.string().optional(),
  branch: z.string().optional(),
  gitSecretRef: z.object({ name: z.string() }).optional(),
  schedule: z.string().optional(),
  timeZone: z.string().optional(),
  suspend: z.boolean().optional(),
  targets: z.object({
    pullRequests: repositoryMonitorTargetSchema.optional(),
    issues: repositoryMonitorIssueTargetSchema.optional(),
    commits: repositoryMonitorTargetSchema.optional(),
  }).optional(),
  agents: z.object({
    reviewer: agentRefSchema.optional(),
    triager: agentRefSchema.optional(),
    researcher: agentRefSchema.optional(),
    planner: agentRefSchema.optional(),
    repairer: agentRefSchema.optional(),
    implementer: agentRefSchema.optional(),
  }).optional(),
  triggers: repositoryMonitorTriggersSchema.optional(),

  issueWorkflow: z.object({
    triage: z.object({ enabled: z.boolean().optional() }).optional(),
    research: z.object({ enabled: z.boolean().optional() }).optional(),
    planning: z.object({
      enabled: z.boolean().optional(),
      requireHumanApprovalFor: z.array(z.string()).optional(),
    }).optional(),
    implementation: z.object({
      enabled: z.boolean().optional(),
      requireApprovedPlan: z.boolean().optional(),
      branchPrefix: z.string().optional(),
      maxActive: z.number().optional(),
      maxAttemptsPerIssue: z.number().optional(),
      maxChangedFiles: z.number().optional(),
      allowedPaths: z.array(z.string()).optional(),
    }).optional(),
  }).optional(),
  review: z.object({
    event: z.string().optional(),
    requireGreenCI: z.boolean().optional(),
    staleReviewTTL: z.string().optional(),
    exactEventEnabled: z.boolean().optional(),
    publish: z.object({
      enabled: z.boolean().optional(),
      mode: z.string().optional(),
      event: z.string().optional(),
      postPassed: z.boolean().optional(),
      postNeedsChanges: z.boolean().optional(),
      postNeedsHuman: z.boolean().optional(),
      postSecuritySensitive: z.boolean().optional(),
      sameHeadPolicy: z.string().optional(),
      inline: z.object({
        enabled: z.boolean().optional(),
        minPriority: z.string().optional(),
        maxComments: z.number().optional(),
      }).optional(),
    }).optional(),
  }).optional(),
  repair: z.object({
    enabled: z.boolean().optional(),
  }).optional(),
  automerge: z.object({
    enabled: z.boolean().optional(),
    requireGlobalMergeGate: z.boolean().optional(),
    allowedMergeMethods: z.array(z.string()).optional(),
  }).optional(),
  policy: z.object({
    protectedLabels: z.array(z.string()).optional(),
    pauseLabels: z.array(z.string()).optional(),
    allowedRepositoryPermissions: z.array(z.string()).optional(),
  }).optional(),
  validation: z.object({
    image: z.string().optional(),
    mode: z.string().optional(),
    commands: z.array(z.string()).optional(),
  }).optional(),
})

const repositoryMonitorStatusSchema = z.object({
  phase: z.string().optional(),
  lastRunID: z.string().optional(),
  lastRunTime: z.string().optional(),
  lastSuccessfulRunTime: z.string().optional(),
  observedGeneration: z.number().optional(),
  openPullRequests: z.number().optional(),
  pendingReviews: z.number().optional(),
  activeRepairs: z.number().optional(),
  blockedItems: z.number().optional(),
  mergeReadyItems: z.number().optional(),
  openIssues: z.number().optional(),
  pendingIssueActions: z.number().optional(),
  blockedIssues: z.number().optional(),
})

export const repositoryMonitorSchema = z.object({
  apiVersion: z.string().optional(),
  kind: z.string().optional(),
  metadata: k8sMetadataSchema,
  spec: repositoryMonitorSpecSchema,
  status: repositoryMonitorStatusSchema.optional(),
})

export const monitorRunSchema = z.object({
  id: z.string(),
  monitorNamespace: z.string(),
  monitorName: z.string(),
  trigger: z.string(),
  targetKind: z.string().optional(),
  targetNumber: z.number().optional(),
  targetSHA: z.string().optional(),
  commandEventID: z.string().optional(),
  phase: z.string(),
  startedAt: z.string(),
  completedAt: z.string().optional(),
  selectedCount: z.number().optional(),
  createdTaskCount: z.number().optional(),
  skippedCount: z.number().optional(),
  error: z.string().optional(),
})

export const monitorItemSchema = z.object({
  monitorNamespace: z.string(),
  monitorName: z.string(),
  kind: z.string(),
  itemKey: z.string(),
  number: z.number().optional(),
  sha: z.string().optional(),
  title: z.string().optional(),
  body: z.string().optional(),
  htmlURL: z.string().optional(),
  author: z.string().optional(),
  state: z.string().optional(),
  labelsJSON: z.string().optional(),
  snapshotDigest: z.string().optional(),
  githubUpdatedAt: z.string().optional(),
  workflowPhase: z.string().optional(),
  linkedPRNumber: z.number().optional(),
  lastCommandID: z.string().optional(),
  lastCommandIntent: z.string().optional(),
  lastActionID: z.string().optional(),
  lastActionKind: z.string().optional(),
  lastActionTaskName: z.string().optional(),
  baseBranch: z.string().optional(),
  headBranch: z.string().optional(),
  headSHA: z.string().optional(),
  ciState: z.string().optional(),
  skipReason: z.string().optional(),
  lastVerdict: z.string().optional(),
  repairState: z.string().optional(),
  automergeState: z.string().optional(),
  lastPublishID: z.string().optional(),
  lastPublishPhase: z.string().optional(),
  lastPublishReason: z.string().optional(),
  lastPublishURL: z.string().optional(),
  updatedAt: z.string(),
  lastSeenAt: z.string(),
})

export const monitorActionSchema = z.object({
  id: z.string(),
  monitorNamespace: z.string(),
  monitorName: z.string(),
  kind: z.string(),
  number: z.number().optional(),
  actionKind: z.string(),
  snapshotDigest: z.string().optional(),
  headSHA: z.string().optional(),
  taskName: z.string().optional(),
  commandEventID: z.string().optional(),
  workActionID: z.string().optional(),
  monitorGeneration: z.number().optional(),
  verdict: z.string().optional(),
  confidence: z.string().optional(),
  summary: z.string().optional(),
  payloadJSON: z.string().optional(),
  payloadDigest: z.string().optional(),
  createdAt: z.string(),
})

export const monitorWorkActionSchema = z.object({
  id: z.string(),
  monitorNamespace: z.string(),
  monitorName: z.string(),
  runID: z.string().optional(),
  commandEventID: z.string().optional(),
  monitorGeneration: z.number().optional(),
  targetKind: z.string().optional(),
  targetNumber: z.number().optional(),
  targetSHA: z.string().optional(),
  targetSnapshotDigest: z.string().optional(),
  intent: z.string().optional(),
  desiredAction: z.string().optional(),
  dependsOnActionID: z.string().optional(),
  dedupeKey: z.string().optional(),
  idempotencyKey: z.string().optional(),
  status: z.string(),
  phase: z.string().optional(),
  attempt: z.number(),
  leaseOwner: z.string().optional(),
  leaseExpiresAt: z.string().optional(),
  taskName: z.string().optional(),
  blockedReason: z.string().optional(),
  error: z.string().optional(),
  artifactIDs: z.string().optional(),
  payloadDigest: z.string().optional(),
  metadataJSON: z.string().optional(),
  createdAt: z.string(),
  updatedAt: z.string(),
  completedAt: z.string().optional(),
})

export const monitorImplementationJobSchema = z.object({
  id: z.string(),
  monitorNamespace: z.string(),
  monitorName: z.string(),
  repo: z.string().optional(),
  issueNumber: z.number().optional(),
  planID: z.string().optional(),
  snapshotDigest: z.string().optional(),
  phase: z.string().optional(),
  attempt: z.number(),
  branch: z.string().optional(),
  patchArtifactID: z.string().optional(),
  prNumber: z.number().optional(),
  validationState: z.string().optional(),
  taskName: z.string().optional(),
  mutationTaskName: z.string().optional(),
  commandEventID: z.string().optional(),
  workActionID: z.string().optional(),
  monitorGeneration: z.number().optional(),
  error: z.string().optional(),
  createdAt: z.string(),
  updatedAt: z.string(),
  completedAt: z.string().optional(),
})

export const monitorMutationSchema = z.object({
  id: z.string(),
  monitorNamespace: z.string(),
  monitorName: z.string(),
  runID: z.string().optional(),
  commandEventID: z.string().optional(),
  workActionID: z.string().optional(),
  monitorGeneration: z.number().optional(),
  operation: z.string(),
  targetKind: z.string().optional(),
  targetNumber: z.number().optional(),
  targetSHA: z.string().optional(),
  actor: z.string().optional(),
  reason: z.string().optional(),
  requestDigest: z.string().optional(),
  githubURL: z.string().optional(),
  githubRequestID: z.string().optional(),
  externalID: z.string().optional(),
  status: z.string().optional(),
  error: z.string().optional(),
  createdAt: z.string(),
})

export const monitorCommandSchema = z.object({
  id: z.string(),
  monitorNamespace: z.string(),
  monitorName: z.string(),
  repo: z.string().optional(),
  kind: z.string().optional(),
  number: z.number().optional(),
  source: z.string().optional(),
  deliveryID: z.string().optional(),
  label: z.string().optional(),
  monitorGeneration: z.number().optional(),
  dedupeKey: z.string().optional(),
  idempotencyKey: z.string().optional(),
  commentID: z.string().optional(),
  commentURL: z.string().optional(),
  author: z.string().optional(),
  authorAssociation: z.string().optional(),
  permission: z.string().optional(),
  command: z.string().optional(),
  intent: z.string().optional(),
  headSHA: z.string().optional(),
  issueSnapshotDigest: z.string().optional(),
  status: z.string().optional(),
  statusCommentID: z.string().optional(),
  createdRepairJobID: z.string().optional(),
  createdAt: z.string(),
  processedAt: z.string().optional(),
  error: z.string().optional(),
})

export type RepositoryMonitor = z.infer<typeof repositoryMonitorSchema>
export type MonitorRun = z.infer<typeof monitorRunSchema>
export type MonitorItem = z.infer<typeof monitorItemSchema>
export type MonitorAction = z.infer<typeof monitorActionSchema>
export type MonitorWorkAction = z.infer<typeof monitorWorkActionSchema>
export type MonitorImplementationJob = z.infer<typeof monitorImplementationJobSchema>
export type MonitorMutation = z.infer<typeof monitorMutationSchema>
export type MonitorCommand = z.infer<typeof monitorCommandSchema>
