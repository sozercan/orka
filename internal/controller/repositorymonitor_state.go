package controller

func repositoryMonitorIssuePhaseTransitionAllowed(from, to string) bool {
	if from == "" || from == to {
		return true
	}
	allowed := map[string]map[string]struct{}{
		repositoryMonitorIssuePhaseDiscovered:           {repositoryMonitorIssuePhaseTriageQueued: {}, repositoryMonitorIssuePhaseResearchQueued: {}, repositoryMonitorIssuePhasePlanQueued: {}, repositoryMonitorIssuePhaseImplementationQueued: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhaseTriageQueued:         {repositoryMonitorIssuePhaseTriaging: {}, repositoryMonitorIssuePhaseBlocked: {}, repositoryMonitorIssuePhaseDiscovered: {}},
		repositoryMonitorIssuePhaseTriaging:             {repositoryMonitorIssuePhaseTriaged: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhaseTriaged:              {repositoryMonitorIssuePhaseResearchQueued: {}, repositoryMonitorIssuePhasePlanQueued: {}, repositoryMonitorIssuePhaseImplementationQueued: {}, repositoryMonitorIssuePhaseBlocked: {}, repositoryMonitorIssuePhaseComplete: {}},
		repositoryMonitorIssuePhaseResearchQueued:       {repositoryMonitorIssuePhaseResearching: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhaseResearching:          {repositoryMonitorIssuePhaseResearched: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhaseResearched:           {repositoryMonitorIssuePhasePlanQueued: {}, repositoryMonitorIssuePhaseImplementationQueued: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhasePlanQueued:           {repositoryMonitorIssuePhasePlanning: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhasePlanning:             {repositoryMonitorIssuePhasePlanReady: {}, repositoryMonitorIssuePhaseApprovalRequired: {}, repositoryMonitorIssuePhaseApproved: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhasePlanReady:            {repositoryMonitorIssuePhaseApprovalRequired: {}, repositoryMonitorIssuePhaseApproved: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhaseApprovalRequired:     {repositoryMonitorIssuePhaseApproved: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhaseApproved:             {repositoryMonitorIssuePhaseImplementationQueued: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhaseImplementationQueued: {repositoryMonitorIssuePhaseImplementing: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhaseImplementing:         {repositoryMonitorIssuePhasePatchReady: {}, repositoryMonitorIssuePhaseMutationQueued: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhasePatchReady:           {repositoryMonitorIssuePhaseMutationQueued: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhaseMutationQueued:       {repositoryMonitorIssuePhaseMutatingToPR: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhaseMutatingToPR:         {repositoryMonitorIssuePhasePROpened: {}, repositoryMonitorIssuePhaseBlocked: {}},
		repositoryMonitorIssuePhaseBlocked:              {repositoryMonitorIssuePhaseDiscovered: {}, repositoryMonitorIssuePhaseTriageQueued: {}, repositoryMonitorIssuePhaseResearchQueued: {}, repositoryMonitorIssuePhasePlanQueued: {}, repositoryMonitorIssuePhaseApproved: {}},
	}
	_, ok := allowed[from][to]
	return ok
}
