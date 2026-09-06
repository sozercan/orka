import { describe, expect, it } from 'vitest'
import {
  monitorActionSchema,
  monitorCommandSchema,
  monitorImplementationJobSchema,
  monitorWorkActionSchema,
  repositoryMonitorSpecSchema,
} from './monitor'

describe('repositoryMonitorSpecSchema', () => {
  it('preserves issue inventory label policy', () => {
    const targets = {
      issues: {
        enabled: true,
        maxPerRun: 25,
        includeLabels: ['agent-ready'],
        excludeLabels: ['blocked'],
      },
    }
    const parsed = repositoryMonitorSpecSchema.parse({ repoURL: 'https://github.com/orka-agents/orka', targets })
    expect(parsed.targets).toEqual(targets)
  })

  it('preserves GitHub label trigger configuration', () => {
    const triggers = {
      github: {
        labels: {
          enabled: true,
          consumeCommandLabels: true,
          requireActorPermission: 'maintain',
          issues: {
            triage: 'orka:triage',
            research: 'orka:research',
            plan: 'orka:plan',
            approvePlan: 'orka:approve-plan',
            implement: 'orka:implement',
            decompose: 'orka:to-issues',
            stop: 'orka:stop',
            resume: 'orka:resume',
          },
          pullRequests: {
            review: 'orka:review',
            fix: 'orka:fix',
            fixCI: 'orka:fix-ci',
            updateBranch: 'orka:update-branch',
            automerge: 'orka:automerge',
            stop: 'orka:stop',
            resume: 'orka:resume',
          },
        },
      },
    }
    const parsed = repositoryMonitorSpecSchema.parse({ repoURL: 'https://github.com/orka-agents/orka', triggers })
    expect(parsed.triggers).toEqual(triggers)
  })

  it('preserves the repository validation image', () => {
    const validation = {
      image: 'ghcr.io/orka-agents/go-ci@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    }
    const parsed = repositoryMonitorSpecSchema.parse({ repoURL: 'https://github.com/orka-agents/orka', validation })
    expect(parsed.validation).toEqual(validation)
  })

  it('preserves legacy validation fields for upgrade diagnostics', () => {
    const validation = {
      mode: 'full',
      commands: ['go test ./...'],
    }
    const parsed = repositoryMonitorSpecSchema.parse({ repoURL: 'https://github.com/orka-agents/orka', validation })
    expect(parsed.validation).toEqual(validation)
  })
})

describe('monitor workflow DTO schemas', () => {
  it('preserves action correlation and integrity fields', () => {
    const action = {
      id: 'act-1',
      monitorNamespace: 'default',
      monitorName: 'orka',
      kind: 'issue',
      actionKind: 'implementation',
      workActionID: 'wa-1',
      monitorGeneration: 7,
      payloadDigest: 'sha256:payload',
      createdAt: '2026-07-16T00:00:00Z',
    }
    expect(monitorActionSchema.parse(action)).toEqual(action)
  })

  it('preserves durable work-action identity and requires attempt', () => {
    const action = {
      id: 'wa-1',
      monitorNamespace: 'default',
      monitorName: 'orka',
      dependsOnActionID: 'wa-0',
      dedupeKey: 'dedupe-1',
      idempotencyKey: 'idempotency-1',
      status: 'queued',
      attempt: 0,
      metadataJSON: '{"source":"test"}',
      createdAt: '2026-07-16T00:00:00Z',
      updatedAt: '2026-07-16T00:00:00Z',
    }
    expect(monitorWorkActionSchema.parse(action)).toEqual(action)
    expect(() => monitorWorkActionSchema.parse({ ...action, attempt: undefined })).toThrow()
  })

  it('requires implementation attempt', () => {
    const job = {
      id: 'impl-1',
      monitorNamespace: 'default',
      monitorName: 'orka',
      attempt: 1,
      createdAt: '2026-07-16T00:00:00Z',
      updatedAt: '2026-07-16T00:00:00Z',
    }
    expect(monitorImplementationJobSchema.parse(job)).toEqual(job)
    expect(() => monitorImplementationJobSchema.parse({ ...job, attempt: undefined })).toThrow()
  })

  it('preserves command provenance and repair linkage', () => {
    const command = {
      id: 'cmd-1',
      monitorNamespace: 'default',
      monitorName: 'orka',
      monitorGeneration: 7,
      dedupeKey: 'dedupe-1',
      idempotencyKey: 'idempotency-1',
      commentID: '123',
      commentURL: 'https://github.com/orka-agents/orka/issues/1#issuecomment-123',
      authorAssociation: 'MEMBER',
      command: 'implement',
      statusCommentID: '456',
      createdRepairJobID: 'repair-1',
      createdAt: '2026-07-16T00:00:00Z',
    }
    expect(monitorCommandSchema.parse(command)).toEqual(command)
  })
})
