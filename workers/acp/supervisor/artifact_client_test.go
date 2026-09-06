package supervisor

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/artifactcap"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const testWorkspaceRelativeRoot = "services/app"

func TestArtifactClientUploadAndDownload(t *testing.T) {
	t.Parallel()
	token := "opaque-operation-capability-never-log"
	authorization := artifactcap.Authorization{Capability: token, RequestDigest: artifactcap.DigestBytes([]byte("request-binding"))}
	uploadData := []byte("workspace delta")
	uploadReference := transportReference(uploadData, artifactcap.MediaTypeWorkspaceDelta)
	downloadData := []byte("workspace baseline")
	downloadReference := transportReference(downloadData, artifactcap.MediaTypeWorkspaceTar)
	var uploadCalls atomic.Int32
	var downloadCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get(artifactcap.CapabilityHeader) != token || request.Header.Get(artifactcap.RequestDigestHeader) != authorization.RequestDigest {
			t.Error("artifact authorization headers were not preserved")
		}
		switch request.Method {
		case http.MethodPut:
			uploadCalls.Add(1)
			if request.URL.Path != mustObjectPath(t, uploadReference.Digest) || request.ContentLength != uploadReference.SizeBytes ||
				request.Header.Get("Content-Type") != uploadReference.MediaType {
				t.Errorf("unexpected upload request: %s %#v", request.URL.Path, request.Header)
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Error(err)
			}
			if !bytes.Equal(body, uploadData) {
				t.Errorf("upload body = %q", body)
			}
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"artifactID": uploadReference.ArtifactID,
				"digest":     uploadReference.Digest,
				"sizeBytes":  uploadReference.SizeBytes,
				"mediaType":  uploadReference.MediaType,
				"createdAt":  time.Now().UTC(),
			})
		case http.MethodGet:
			downloadCalls.Add(1)
			if request.URL.Path != mustObjectPath(t, downloadReference.Digest) || request.Header.Get("Accept") != downloadReference.MediaType {
				t.Errorf("unexpected download request: %s %#v", request.URL.Path, request.Header)
			}
			writer.Header().Set("Content-Type", downloadReference.MediaType)
			writer.Header().Set(artifactcap.ObjectDigestHeader, downloadReference.Digest)
			writer.Header().Set("Content-Length", strconv.Itoa(len(downloadData)))
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write(downloadData)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	var sawDelta atomic.Bool
	provider := ArtifactAuthorizationProviderFunc(func(_ context.Context, request ArtifactAuthorizationRequest) (artifactcap.Authorization, error) {
		if request.Operation == artifactcap.OperationUpload && request.WorkspaceDelta != nil {
			sawDelta.Store(true)
		}
		return authorization, nil
	})
	client, err := newDefaultArtifactClient(server.URL, server.Client(), provider)
	if err != nil {
		t.Fatal(err)
	}
	delta := &harnessv2.CreateWorkspaceDeltaRequest{Limits: harnessv2.WorkspaceDeltaLimits{MaxBytes: 1024, MaxEntries: 10}}
	stored, err := client.Upload(context.Background(), uploadReference, uploadData, delta)
	if err != nil {
		t.Fatal(err)
	}
	if stored != uploadReference || !sawDelta.Load() || uploadCalls.Load() != 1 {
		t.Fatalf("stored=%#v sawDelta=%v calls=%d", stored, sawDelta.Load(), uploadCalls.Load())
	}
	var downloaded bytes.Buffer
	if err := client.Download(context.Background(), downloadReference, &downloaded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(downloaded.Bytes(), downloadData) || downloadCalls.Load() != 1 {
		t.Fatalf("download=%q calls=%d", downloaded.Bytes(), downloadCalls.Load())
	}
}

func TestArtifactClientRejectsDisconnectMismatchRedirectAndRedacts(t *testing.T) {
	t.Parallel()
	data := []byte("expected artifact")
	reference := transportReference(data, artifactcap.MediaTypeWorkspaceTar)
	token := "operation-capability-sensitive-value"
	authorization := artifactcap.Authorization{Capability: token, RequestDigest: artifactcap.DigestBytes([]byte("request-binding"))}
	provider := ArtifactAuthorizationProviderFunc(func(context.Context, ArtifactAuthorizationRequest) (artifactcap.Authorization, error) {
		return authorization, nil
	})
	tests := []struct {
		name    string
		handler http.Handler
	}{
		{
			name: "disconnect",
			handler: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", reference.MediaType)
				writer.Header().Set(artifactcap.ObjectDigestHeader, reference.Digest)
				writer.Header().Set("Content-Length", strconv.FormatInt(reference.SizeBytes, 10))
				writer.WriteHeader(http.StatusOK)
				_, _ = writer.Write(data[:4])
			}),
		},
		{
			name: "digest mismatch",
			handler: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", reference.MediaType)
				writer.Header().Set(artifactcap.ObjectDigestHeader, reference.Digest)
				writer.Header().Set("Content-Length", strconv.FormatInt(reference.SizeBytes, 10))
				writer.WriteHeader(http.StatusOK)
				_, _ = writer.Write(bytes.Repeat([]byte{'x'}, int(reference.SizeBytes)))
			}),
		},
		{
			name: "authorization response redaction",
			handler: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(http.StatusForbidden)
				_, _ = writer.Write([]byte(token))
			}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()
			client, err := newDefaultArtifactClient(server.URL, server.Client(), provider)
			if err != nil {
				t.Fatal(err)
			}
			var destination bytes.Buffer
			err = client.Download(context.Background(), reference, &destination)
			if err == nil {
				t.Fatal("expected download failure")
			}
			if strings.Contains(err.Error(), token) {
				t.Fatalf("client error disclosed capability: %q", err)
			}
		})
	}

	var followed atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Store(true) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	client, err := newDefaultArtifactClient(redirect.URL, redirect.Client(), provider)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Download(context.Background(), reference, io.Discard); err == nil {
		t.Fatal("expected redirect refusal")
	}
	if followed.Load() {
		t.Fatal("artifact client followed redirect and risked capability disclosure")
	}
}

func TestRemoteWorkspaceMaterializerRejectsTraversalSymlinkAndUnsafeDestination(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		archive []byte
	}{
		{"traversal", tarBytes(t, tarEntry{name: "../escape", body: []byte("bad")})},
		{"escaping-symlink", tarBytes(t, tarEntry{name: "link", typeFlag: tar.TypeSymlink, linkName: "../../outside"})},
		{"absolute-symlink", tarBytes(t, tarEntry{name: "link", typeFlag: tar.TypeSymlink, linkName: "/etc/passwd"})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			materializer, workspace := materializerForArchive(t, test.archive)
			parent := t.TempDir()
			destination := filepath.Join(parent, "workspace")
			if err := os.Mkdir(destination, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := materializer.Materialize(context.Background(), workspace, destination); err == nil {
				t.Fatal("expected unsafe archive rejection")
			}
			if _, err := os.Stat(filepath.Join(parent, "escape")); !os.IsNotExist(err) {
				t.Fatalf("archive escaped workspace: %v", err)
			}
		})
	}

	goodArchive := tarBytes(t, tarEntry{name: "README.md", body: []byte("safe")})
	materializer, workspace := materializerForArchive(t, goodArchive)
	parent := t.TempDir()
	outside := t.TempDir()
	destination := filepath.Join(parent, "workspace")
	if err := os.Symlink(outside, destination); err != nil {
		t.Fatal(err)
	}
	if err := materializer.Materialize(context.Background(), workspace, destination); err == nil {
		t.Fatal("expected symlink destination rejection")
	}
	entries, err := os.ReadDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("materializer followed destination symlink: %v", entries)
	}
}

func TestRemoteWorkspaceMaterializerEnforcesRelativeRootBoundary(t *testing.T) {
	t.Parallel()
	archive := tarBytes(t,
		tarEntry{name: "services/app/main.go", body: []byte("package main\n")},
		tarEntry{name: "services/shared/secret.txt", body: []byte("outside\n")},
		tarEntry{name: "README.md", body: []byte("root\n")},
	)
	materializer, workspace := materializerForArchive(t, archive)
	workspace.Workspace.RelativeRoot = testWorkspaceRelativeRoot
	destination := filepath.Join(t.TempDir(), "workspace")
	if err := os.Mkdir(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := materializer.Materialize(context.Background(), workspace, destination); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(destination, "main.go"))
	if err != nil || string(content) != "package main\n" {
		t.Fatalf("relative-root content = %q, %v", content, err)
	}
	for _, outside := range []string{"README.md", "services", "secret.txt"} {
		if _, err := os.Lstat(filepath.Join(destination, outside)); !os.IsNotExist(err) {
			t.Fatalf("path outside relative root %q was exposed: %v", outside, err)
		}
	}
}

func TestRemoteWorkspaceMaterializerRejectsRelativeRootSymlinkEscape(t *testing.T) {
	t.Parallel()
	archive := tarBytes(t,
		tarEntry{name: "app/main.go", body: []byte("package main\n")},
		tarEntry{name: "outside.txt", body: []byte("outside\n")},
		tarEntry{name: "app/outside", typeFlag: tar.TypeSymlink, linkName: "../outside.txt"},
	)
	materializer, workspace := materializerForArchive(t, archive)
	workspace.Workspace.RelativeRoot = "app"
	destination := filepath.Join(t.TempDir(), "workspace")
	if err := os.Mkdir(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := materializer.Materialize(context.Background(), workspace, destination); err == nil || !strings.Contains(err.Error(), "outside its boundary") {
		t.Fatalf("relative-root symlink escape error = %v", err)
	}
}

func TestRemoteWorkspaceMaterializerRejectsSymlinkInRelativeRootPath(t *testing.T) {
	t.Parallel()
	archive := tarBytes(t,
		tarEntry{name: "app", typeFlag: tar.TypeDir},
		tarEntry{name: "private", typeFlag: tar.TypeDir},
		tarEntry{name: "private/subdir", typeFlag: tar.TypeDir},
		tarEntry{name: "private/subdir/secret.txt", body: []byte("outside\n")},
		tarEntry{name: "app/link", typeFlag: tar.TypeSymlink, linkName: "../private"},
	)
	materializer, workspace := materializerForArchive(t, archive)
	workspace.Workspace.RelativeRoot = "app/link/subdir"
	destination := filepath.Join(t.TempDir(), "workspace")
	if err := os.Mkdir(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := materializer.Materialize(context.Background(), workspace, destination); err == nil ||
		!strings.Contains(err.Error(), "relative root component") {
		t.Fatalf("intermediate relative-root symlink error = %v", err)
	}
}

func TestRemoteWorkspaceMaterializerAndUploader(t *testing.T) {
	t.Parallel()
	archive := tarBytes(t,
		tarEntry{name: ".agents", typeFlag: tar.TypeDir},
		tarEntry{name: ".agents/skills", typeFlag: tar.TypeDir},
		tarEntry{name: ".agents/skills/readme", body: []byte("skill\n")},
		tarEntry{name: ".claude", typeFlag: tar.TypeDir},
		tarEntry{name: ".claude/skills", typeFlag: tar.TypeDir},
		tarEntry{name: ".claude/skills/readme", typeFlag: tar.TypeSymlink, linkName: "../../.agents/skills/readme"},
		tarEntry{name: "src", typeFlag: tar.TypeDir},
		tarEntry{name: "src/main.go", body: []byte("package main\n")},
	)
	materializer, workspace := materializerForArchive(t, archive)
	destination := filepath.Join(t.TempDir(), "workspace")
	if err := os.Mkdir(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := materializer.Materialize(context.Background(), workspace, destination); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(destination, "src", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "package main\n" {
		t.Fatalf("materialized content = %q", content)
	}
	linkPath := filepath.Join(destination, ".claude", "skills", "readme")
	info, err := os.Lstat(linkPath)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("materialized safe symlink info = %#v, %v", info, err)
	}
	target, err := os.Readlink(linkPath)
	if err != nil || target != "../../.agents/skills/readme" {
		t.Fatalf("materialized safe symlink target = %q, %v", target, err)
	}

	deltaData := []byte("delta archive")
	deltaDigest := artifactcap.DigestBytes(deltaData)
	var uploaded bytes.Buffer
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(&uploaded, request.Body)
		artifactID, _ := artifactcap.ArtifactIDForDigest(deltaDigest)
		writer.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"artifactID": artifactID, "digest": deltaDigest, "sizeBytes": len(deltaData),
			"mediaType": artifactcap.MediaTypeWorkspaceDelta, "createdAt": time.Now().UTC(),
		})
	}))
	defer server.Close()
	provider := ArtifactAuthorizationProviderFunc(func(_ context.Context, request ArtifactAuthorizationRequest) (artifactcap.Authorization, error) {
		if request.WorkspaceDelta == nil {
			return artifactcap.Authorization{}, fmt.Errorf("missing workspace delta context")
		}
		return artifactcap.Authorization{Capability: "token", RequestDigest: artifactcap.DigestBytes([]byte("binding"))}, nil
	})
	client, err := newDefaultArtifactClient(server.URL, server.Client(), provider)
	if err != nil {
		t.Fatal(err)
	}
	uploader, err := NewRemoteArtifactUploader(client)
	if err != nil {
		t.Fatal(err)
	}
	request := harnessv2.CreateWorkspaceDeltaRequest{
		Limits: harnessv2.WorkspaceDeltaLimits{MaxBytes: 1024, MaxEntries: 10},
	}
	reference, err := uploader.UploadWorkspaceDelta(context.Background(), request, deltaData, deltaDigest)
	if err != nil {
		t.Fatal(err)
	}
	if reference.Digest != deltaDigest || !bytes.Equal(uploaded.Bytes(), deltaData) {
		t.Fatalf("reference=%#v uploaded=%q", reference, uploaded.Bytes())
	}
}

func materializerForArchive(t *testing.T, archive []byte) (WorkspaceMaterializer, harnessv2.CreateRuntimeSessionRequest) {
	t.Helper()
	reference := transportReference(archive, artifactcap.MediaTypeWorkspaceTar)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", reference.MediaType)
		writer.Header().Set(artifactcap.ObjectDigestHeader, reference.Digest)
		writer.Header().Set("Content-Length", strconv.Itoa(len(archive)))
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(archive)
	}))
	t.Cleanup(server.Close)
	provider := ArtifactAuthorizationProviderFunc(func(context.Context, ArtifactAuthorizationRequest) (artifactcap.Authorization, error) {
		return artifactcap.Authorization{Capability: "token", RequestDigest: artifactcap.DigestBytes([]byte("binding"))}, nil
	})
	client, err := newDefaultArtifactClient(server.URL, server.Client(), provider)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewRemoteWorkspaceMaterializer(client, WorkspaceMaterializerLimits{MaxEntries: 100, MaxExpandedBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	request := harnessv2.CreateRuntimeSessionRequest{
		Workspace: harnessv2.WorkspaceSpec{
			Intent: harnessv2.WorkspaceIntentRead,
			Baseline: harnessv2.WorkspaceBaseline{
				RepositoryIdentity: "github.com/example/repo", Revision: "0123456789abcdef",
				TreeDigest: artifactcap.DigestBytes([]byte("tree")), Artifact: &reference,
			},
		},
		WorkspaceArtifactAuthorization: &harnessv2.ArtifactAuthorization{Capability: "token", RequestDigest: artifactcap.DigestBytes([]byte("binding"))},
	}
	return materializer, request
}

type tarEntry struct {
	name     string
	body     []byte
	typeFlag byte
	linkName string
}

func tarBytes(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for _, entry := range entries {
		typeFlag := entry.typeFlag
		if typeFlag == 0 {
			typeFlag = tar.TypeReg
		}
		size := int64(len(entry.body))
		if typeFlag != tar.TypeReg {
			size = 0
		}
		header := &tar.Header{Name: entry.name, Typeflag: typeFlag, Size: size, Mode: 0o644, Linkname: entry.linkName}
		if typeFlag == tar.TypeDir {
			header.Mode = 0o755
		}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if size > 0 {
			if _, err := writer.Write(entry.body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func transportReference(data []byte, mediaType string) harnessv2.ArtifactReference {
	digest := artifactcap.DigestBytes(data)
	artifactID, _ := artifactcap.ArtifactIDForDigest(digest)
	return harnessv2.ArtifactReference{
		ArtifactID: harnessv2.ArtifactID(artifactID), Digest: digest,
		SizeBytes: int64(len(data)), MediaType: mediaType,
	}
}

func mustObjectPath(t *testing.T, digest string) string {
	t.Helper()
	value, err := artifactcap.ObjectPath(digest)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// newDefaultArtifactClient builds an ArtifactClient with the production
// transfer limits env.go applies when constructing the supervisor.
func newDefaultArtifactClient(baseURL string, client *http.Client, authorization ArtifactAuthorizationProvider) (*ArtifactClient, error) {
	return newArtifactClient(baseURL, client, authorization, artifactClientLimits{
		MaxDownloadBytes: defaultWorkspaceArtifactDownloadBytes,
		MaxUploadBytes:   defaultWorkspaceDeltaUploadBytes,
	})
}
