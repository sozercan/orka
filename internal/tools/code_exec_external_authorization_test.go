/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestKubernetesCodeExecTimeoutPreservesPodLogAuthorization(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		t.Run(map[bool]string{false: "denied", true: "allowed"}[allowed], func(t *testing.T) {
			const namespace, jobName, podName = "request-ns", "sandbox-job", "sandbox-pod"
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			backend := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace, Labels: map[string]string{codeExecKubernetesLabelJob: jobName}},
			}).Build()
			authorizations := 0
			toolCtx := &ToolContext{Client: backend, Namespace: namespace, AuthorizePodLogs: func(ctx context.Context, actualNamespace, actualPod string) error {
				authorizations++
				if ctx.Err() != nil || actualNamespace != namespace || actualPod != podName {
					t.Errorf("log authorization did not use the final Pod and independent timeout context")
				}
				if !allowed {
					return errors.New("pod logs denied")
				}
				return nil
			}}
			streamer := &externalAuthorizationLogStreamer{logs: fakeKubernetesCodeExecLogs(jobName, "authorized output", "")}
			executor := &KubernetesJobCodeExecutor{logStreamer: streamer}
			ctx, cancel := context.WithCancel(WithToolContext(context.Background(), toolCtx))
			clients, err := executor.kubernetesClients(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			// Timeout collection replaces the canceled tool context with a fresh
			// context. The captured caller authorizer must survive that handoff.
			result := executor.timeoutResult(clients, jobName, 1024)
			if !result.TimedOut || authorizations != 1 {
				t.Fatalf("timeout=%v authorizations=%d, want true and one authorization", result.TimedOut, authorizations)
			}
			if allowed {
				if streamer.calls != 1 || result.Output != "authorized output" {
					t.Fatalf("authorized logs not read: calls=%d output=%q", streamer.calls, result.Output)
				}
			} else if streamer.calls != 0 || result.Output != "" || !strings.Contains(result.Error, "pod logs denied") {
				t.Fatalf("denied logs reached streamer: calls=%d output=%q error=%s", streamer.calls, result.Output, result.Error)
			}
		})
	}
}

type externalAuthorizationLogStreamer struct {
	calls int
	logs  string
}

func (s *externalAuthorizationLogStreamer) Stream(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
	s.calls++
	return io.NopCloser(strings.NewReader(s.logs)), nil
}
