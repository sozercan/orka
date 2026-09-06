package main

import "testing"

const (
	doctorTestPhaseReady = "Ready"
	doctorTestKeyPhase   = "phase"
	doctorTestKeySpec    = "spec"
)

func TestMonitorDoctorSummaryKeepsHealthFieldsOnly(t *testing.T) {
	monitor := map[string]any{
		monitorSummaryKeyMetadata: map[string]any{monitorSummaryKeyName: "vekil-monitor", cliNamespaceQuery: configTestNamespace, "managedFields": []any{map[string]any{"manager": "kubectl"}}},
		doctorTestKeySpec:         map[string]any{"repository": "vekil", "repoURL": "https://github.com/example/vekil", "branch": "trunk", "credentialRef": map[string]any{monitorSummaryKeyName: "forge-credential"}},
		monitorSummaryKeyStatus: map[string]any{
			doctorTestKeyPhase: doctorTestPhaseReady, "lastRunID": "monrun-1", "openIssues": float64(16), "blockedItems": float64(3),
			"conditions": []any{map[string]any{monitorSummaryKeyType: doctorTestPhaseReady, monitorSummaryKeyStatus: "True", "reason": "RunSucceeded", "observedGeneration": float64(7)}},
		},
	}
	summary := monitorDoctorSummary(monitor)
	if summary[monitorSummaryKeyName] != "vekil-monitor" || summary["repository"] != "https://github.com/example/vekil" || summary[doctorTestKeyPhase] != doctorTestPhaseReady || summary["openIssues"] != float64(16) {
		t.Fatalf("summary = %#v", summary)
	}
	if _, leaked := summary["managedFields"]; leaked {
		t.Fatalf("summary exposed managed fields: %#v", summary)
	}
	conditions, ok := summary["conditions"].([]map[string]any)
	if !ok || len(conditions) != 1 || conditions[0]["reason"] != "RunSucceeded" {
		t.Fatalf("conditions = %#v", summary["conditions"])
	}
	if _, present := conditions[0]["observedGeneration"]; present {
		t.Fatalf("condition kept an unlisted field: %#v", conditions[0])
	}
	if got := monitorDoctorSummary("not a map"); len(got) != 0 {
		t.Fatalf("non-object summary = %#v", got)
	}
}
