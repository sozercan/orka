import { describe, it, expect } from 'vitest'
import { parseRepositoryURL, repositoryDisplayName } from './repository-name'

describe('repositoryDisplayName', () => {
  it('derives owner/repo from an https URL', () => {
    expect(repositoryDisplayName({ repoURL: 'https://github.com/sozercan/nodejs-goof' }, 'x')).toBe('sozercan/nodejs-goof')
  })
  it('strips .git and trailing slashes', () => {
    expect(repositoryDisplayName({ repoURL: 'https://github.com/sozercan/vekil.git/' }, 'x')).toBe('sozercan/vekil')
  })
  it('handles scp-like git URLs', () => {
    expect(parseRepositoryURL('git@github.com:owner/repo.git')).toEqual({ owner: 'owner', repository: 'repo' })
  })
  it('prefers explicit owner/repository when both are set', () => {
    expect(repositoryDisplayName({ repoURL: 'https://github.com/a/b', owner: 'c', repository: 'd' }, 'x')).toBe('c/d')
  })
  it('falls back to the resource name for unparseable URLs', () => {
    expect(repositoryDisplayName({ repoURL: 'not a url' }, 'demo-repo')).toBe('demo-repo')
    expect(repositoryDisplayName({}, 'demo-repo')).toBe('demo-repo')
  })
})
