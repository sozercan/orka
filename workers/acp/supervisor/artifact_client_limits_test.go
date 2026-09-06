package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orka-agents/orka/internal/artifactcap"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

type artifactLimitRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f artifactLimitRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type repeatedByteReader byte

func (r repeatedByteReader) Read(buffer []byte) (int, error) {
	for i := range buffer {
		buffer[i] = byte(r)
	}
	return len(buffer), nil
}

func TestArtifactClientUsesSeparateBoundedDefaults(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client, err := newDefaultArtifactClient("https://artifact.example", &http.Client{Transport: artifactLimitRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, fmt.Errorf("unexpected artifact transport call")
	})}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if client.maxDownloadBytes != defaultWorkspaceArtifactDownloadBytes || client.maxDownloadBytes <= 0 {
		t.Fatalf("artifact download limit = %d, want bounded default %d", client.maxDownloadBytes, defaultWorkspaceArtifactDownloadBytes)
	}
	wantUpload := defaultProtocolLimits(providerKindCodex).MaxWorkspaceDeltaBytes
	if client.maxUploadBytes != wantUpload || client.maxUploadBytes <= 0 {
		t.Fatalf("artifact upload limit = %d, want outbound capability %d", client.maxUploadBytes, wantUpload)
	}

	reference := artifactReferenceWithSize(defaultWorkspaceArtifactDownloadBytes+1, artifactcap.MediaTypeWorkspaceTar)
	err = client.DownloadAuthorized(context.Background(), reference, testArtifactAuthorization(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "exceeds transport limit") {
		t.Fatalf("oversized default-limit download error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("artifact transport calls = %d, want 0", calls.Load())
	}
}

func TestArtifactClientDownloadsRaisedPublisherWorkspaceArtifact(t *testing.T) {
	t.Parallel()

	const extraBytes = int64(1 << 20)
	artifactSize := defaultWorkspaceArtifactDownloadBytes + extraBytes
	reference := repeatedByteArtifactReference(t, artifactSize, 0, artifactcap.MediaTypeWorkspaceTar)
	var calls atomic.Int32
	client, err := newArtifactClient(
		"https://artifact.example",
		&http.Client{Transport: artifactLimitRoundTripperFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			header := make(http.Header)
			header.Set("Content-Type", reference.MediaType)
			header.Set(artifactcap.ObjectDigestHeader, reference.Digest)
			return &http.Response{
				StatusCode:    http.StatusOK,
				Body:          io.NopCloser(io.LimitReader(repeatedByteReader(0), reference.SizeBytes)),
				Header:        header,
				ContentLength: reference.SizeBytes,
			}, nil
		})},
		nil,
		artifactClientLimits{
			MaxDownloadBytes: artifactSize,
			MaxUploadBytes:   defaultWorkspaceDeltaUploadBytes,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.DownloadAuthorized(context.Background(), reference, testArtifactAuthorization(), io.Discard); err != nil {
		t.Fatalf("download valid %d-byte Publisher workspace artifact: %v", artifactSize, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("artifact transport calls = %d, want 1", calls.Load())
	}

	outbound := artifactReferenceWithSize(artifactSize, artifactcap.MediaTypeWorkspaceDelta)
	if _, err := client.UploadAuthorized(context.Background(), outbound, testArtifactAuthorization(), nil); err == nil || !strings.Contains(err.Error(), "exceeds transport limit") {
		t.Fatalf("outbound delta above independent upload capability error = %v", err)
	}
}

func TestArtifactClientRejectsInvalidTransferLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		limits artifactClientLimits
	}{
		{name: "zero download", limits: artifactClientLimits{MaxUploadBytes: 1}},
		{name: "negative download", limits: artifactClientLimits{MaxDownloadBytes: -1, MaxUploadBytes: 1}},
		{name: "zero upload", limits: artifactClientLimits{MaxDownloadBytes: 1}},
		{name: "negative upload", limits: artifactClientLimits{MaxDownloadBytes: 1, MaxUploadBytes: -1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := newArtifactClient("https://artifact.example", nil, nil, test.limits)
			if err == nil || client != nil || !strings.Contains(err.Error(), "must be positive") {
				t.Fatalf("newArtifactClient(%+v) = %#v, %v", test.limits, client, err)
			}
		})
	}
}

func TestArtifactClientEnforcesConfiguredTransferLimitsByDirection(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client, err := newArtifactClient(
		"https://artifact.example",
		&http.Client{Transport: artifactLimitRoundTripperFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return nil, fmt.Errorf("unexpected artifact transport call")
		})},
		nil,
		artifactClientLimits{MaxDownloadBytes: 6, MaxUploadBytes: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("12345")
	upload := transportReference(data, artifactcap.MediaTypeWorkspaceDelta)
	if _, err := client.UploadAuthorized(context.Background(), upload, testArtifactAuthorization(), data); err == nil || !strings.Contains(err.Error(), "exceeds transport limit") {
		t.Fatalf("oversized upload error = %v", err)
	}
	download := upload
	download.MediaType = artifactcap.MediaTypeWorkspaceTar
	if err := client.DownloadAuthorized(context.Background(), download, testArtifactAuthorization(), io.Discard); err == nil || !strings.Contains(err.Error(), "transport failed") {
		t.Fatalf("download inside independent limit error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("artifact transport calls = %d, want 1 download call", calls.Load())
	}
}

func repeatedByteArtifactReference(t *testing.T, size int64, value byte, mediaType string) harnessv2.ArtifactReference {
	t.Helper()
	hash := sha256.New()
	if _, err := io.CopyN(hash, repeatedByteReader(value), size); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	artifactID, err := artifactcap.ArtifactIDForDigest(digest)
	if err != nil {
		t.Fatal(err)
	}
	return harnessv2.ArtifactReference{
		ArtifactID: harnessv2.ArtifactID(artifactID),
		Digest:     digest,
		SizeBytes:  size,
		MediaType:  mediaType,
	}
}

func artifactReferenceWithSize(size int64, mediaType string) harnessv2.ArtifactReference {
	digest := artifactcap.DigestBytes(fmt.Appendf(nil, "artifact-size-%d-%s", size, mediaType))
	artifactID, _ := artifactcap.ArtifactIDForDigest(digest)
	return harnessv2.ArtifactReference{
		ArtifactID: harnessv2.ArtifactID(artifactID),
		Digest:     digest,
		SizeBytes:  size,
		MediaType:  mediaType,
	}
}

func testArtifactAuthorization() artifactcap.Authorization {
	return artifactcap.Authorization{
		Capability:    "test-operation-capability",
		RequestDigest: artifactcap.DigestBytes([]byte("test-request-binding")),
	}
}
