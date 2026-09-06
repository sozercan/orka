package supervisor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/artifactcap"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const maxArtifactClientResponseBytes = 32 << 10

type artifactClientLimits struct {
	MaxDownloadBytes int64
	MaxUploadBytes   int64
}

type ArtifactAuthorizationRequest struct {
	Operation      artifactcap.Operation
	Reference      harnessv2.ArtifactReference
	WorkspaceDelta *harnessv2.CreateWorkspaceDeltaRequest
}

type ArtifactAuthorizationProvider interface {
	AuthorizeArtifact(context.Context, ArtifactAuthorizationRequest) (artifactcap.Authorization, error)
}

type ArtifactAuthorizationProviderFunc func(context.Context, ArtifactAuthorizationRequest) (artifactcap.Authorization, error)

func (f ArtifactAuthorizationProviderFunc) AuthorizeArtifact(ctx context.Context, request ArtifactAuthorizationRequest) (artifactcap.Authorization, error) {
	return f(ctx, request)
}

type ArtifactClient struct {
	baseURL          *url.URL
	httpClient       *http.Client
	authorization    ArtifactAuthorizationProvider
	maxDownloadBytes int64
	maxUploadBytes   int64
}

func newArtifactClient(baseURL string, client *http.Client, authorization ArtifactAuthorizationProvider, limits artifactClientLimits) (*ArtifactClient, error) {
	if limits.MaxDownloadBytes <= 0 || limits.MaxUploadBytes <= 0 {
		return nil, fmt.Errorf("artifact transfer limits must be positive")
	}
	if limits.MaxDownloadBytes == math.MaxInt64 || limits.MaxUploadBytes == math.MaxInt64 {
		return nil, fmt.Errorf("artifact transfer limits must be less than %d", int64(math.MaxInt64))
	}
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("artifact API base URL is invalid")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return nil, fmt.Errorf("artifact API base URL must not contain a path")
	}
	if client == nil {
		// Authenticated in-cluster artifact transfers must never traverse an
		// inherited environment proxy.
		client = &http.Client{Timeout: 2 * time.Minute, Transport: harnessv2.NewProxylessTransport()}
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	parsed.Path = ""
	return &ArtifactClient{
		baseURL: parsed, httpClient: &clientCopy, authorization: authorization,
		maxDownloadBytes: limits.MaxDownloadBytes, maxUploadBytes: limits.MaxUploadBytes,
	}, nil
}

func (c *ArtifactClient) Download(ctx context.Context, reference harnessv2.ArtifactReference, destination io.Writer) error {
	if c.authorization == nil {
		return fmt.Errorf("artifact authorization provider is required")
	}
	authorization, err := c.authorization.AuthorizeArtifact(ctx, ArtifactAuthorizationRequest{Operation: artifactcap.OperationDownload, Reference: reference})
	if err != nil {
		return fmt.Errorf("authorize artifact download")
	}
	return c.DownloadAuthorized(ctx, reference, authorization, destination)
}

func (c *ArtifactClient) DownloadAuthorized(ctx context.Context, reference harnessv2.ArtifactReference, authorization artifactcap.Authorization, destination io.Writer) error {
	if destination == nil {
		return fmt.Errorf("artifact download destination is required")
	}
	if err := validateTransportReference(reference); err != nil {
		return err
	}
	if reference.SizeBytes > c.maxDownloadBytes {
		return fmt.Errorf("artifact download exceeds transport limit")
	}
	request, err := c.newRequest(ctx, http.MethodGet, reference, authorization, nil)
	if err != nil {
		return err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("artifact download transport failed")
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusOK {
		drainBounded(response.Body)
		return fmt.Errorf("artifact download failed with status %d", response.StatusCode)
	}
	if err := validateDownloadHeaders(response, reference); err != nil {
		return err
	}
	hash := sha256.New()
	written, err := io.CopyBuffer(io.MultiWriter(destination, hash), io.LimitReader(response.Body, reference.SizeBytes+1), make([]byte, 128<<10))
	if err != nil {
		return fmt.Errorf("artifact download stream failed")
	}
	if written != reference.SizeBytes {
		return fmt.Errorf("artifact download length mismatch")
	}
	actualDigest := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if !constantArtifactStringEqual(actualDigest, reference.Digest) {
		return fmt.Errorf("artifact download digest mismatch")
	}
	return nil
}

func (c *ArtifactClient) Upload(ctx context.Context, reference harnessv2.ArtifactReference, data []byte, delta *harnessv2.CreateWorkspaceDeltaRequest) (harnessv2.ArtifactReference, error) {
	if c.authorization == nil {
		return harnessv2.ArtifactReference{}, fmt.Errorf("artifact authorization provider is required")
	}
	authorization, err := c.authorization.AuthorizeArtifact(ctx, ArtifactAuthorizationRequest{Operation: artifactcap.OperationUpload, Reference: reference, WorkspaceDelta: delta})
	if err != nil {
		return harnessv2.ArtifactReference{}, fmt.Errorf("authorize artifact upload")
	}
	return c.UploadAuthorized(ctx, reference, authorization, data)
}

func (c *ArtifactClient) UploadAuthorized(ctx context.Context, reference harnessv2.ArtifactReference, authorization artifactcap.Authorization, data []byte) (harnessv2.ArtifactReference, error) {
	if err := validateTransportReference(reference); err != nil {
		return harnessv2.ArtifactReference{}, err
	}
	if reference.SizeBytes > c.maxUploadBytes {
		return harnessv2.ArtifactReference{}, fmt.Errorf("artifact upload exceeds transport limit")
	}
	if int64(len(data)) != reference.SizeBytes || !constantArtifactStringEqual(artifactcap.DigestBytes(data), reference.Digest) {
		return harnessv2.ArtifactReference{}, fmt.Errorf("artifact upload payload does not match reference")
	}
	request, err := c.newRequest(ctx, http.MethodPut, reference, authorization, bytes.NewReader(data))
	if err != nil {
		return harnessv2.ArtifactReference{}, err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return harnessv2.ArtifactReference{}, fmt.Errorf("artifact upload transport failed")
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusCreated {
		drainBounded(response.Body)
		return harnessv2.ArtifactReference{}, fmt.Errorf("artifact upload failed with status %d", response.StatusCode)
	}
	var stored struct {
		ArtifactID string    `json:"artifactID"`
		Digest     string    `json:"digest"`
		SizeBytes  int64     `json:"sizeBytes"`
		MediaType  string    `json:"mediaType"`
		CreatedAt  time.Time `json:"createdAt"`
	}
	responseData, err := io.ReadAll(io.LimitReader(response.Body, maxArtifactClientResponseBytes+1))
	if err != nil || len(responseData) > maxArtifactClientResponseBytes {
		return harnessv2.ArtifactReference{}, fmt.Errorf("artifact upload response is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(responseData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil {
		return harnessv2.ArtifactReference{}, fmt.Errorf("artifact upload response is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return harnessv2.ArtifactReference{}, fmt.Errorf("artifact upload response is invalid")
	}
	storedReference := harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(stored.ArtifactID), Digest: stored.Digest, SizeBytes: stored.SizeBytes, MediaType: stored.MediaType}
	if err := storedReference.Validate(); err != nil || storedReference != reference {
		return harnessv2.ArtifactReference{}, fmt.Errorf("artifact upload response does not match request")
	}
	return storedReference, nil
}

func (c *ArtifactClient) newRequest(ctx context.Context, method string, reference harnessv2.ArtifactReference, authorization artifactcap.Authorization, body io.Reader) (*http.Request, error) {
	if authorization.Capability == "" || !artifactcap.IsRequestDigest(authorization.RequestDigest) {
		return nil, fmt.Errorf("artifact operation authorization is invalid")
	}
	objectPath, err := artifactcap.ObjectPath(reference.Digest)
	if err != nil {
		return nil, err
	}
	endpoint := *c.baseURL
	endpoint.Path = objectPath
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return nil, fmt.Errorf("create artifact request")
	}
	request.Header.Set(artifactcap.CapabilityHeader, authorization.Capability)
	request.Header.Set(artifactcap.RequestDigestHeader, authorization.RequestDigest)
	request.Header.Set(artifactcap.ContentLengthHeader, strconv.FormatInt(reference.SizeBytes, 10))
	request.Header.Set(artifactcap.MediaTypeHeader, reference.MediaType)
	if method == http.MethodPut {
		request.ContentLength = reference.SizeBytes
		request.Header.Set("Content-Type", reference.MediaType)
	} else {
		request.Header.Set("Accept", reference.MediaType)
	}
	return request, nil
}

func validateTransportReference(reference harnessv2.ArtifactReference) error {
	if err := reference.Validate(); err != nil {
		return fmt.Errorf("artifact reference: %w", err)
	}
	expectedID, err := artifactcap.ArtifactIDForDigest(reference.Digest)
	if err != nil || string(reference.ArtifactID) != expectedID {
		return fmt.Errorf("artifact reference is not content addressed")
	}
	if reference.SizeBytes < 0 {
		return fmt.Errorf("artifact reference size is invalid")
	}
	if err := artifactcap.ValidateMediaType(reference.MediaType); err != nil {
		return fmt.Errorf("artifact reference media type is invalid")
	}
	return nil
}

func validateDownloadHeaders(response *http.Response, reference harnessv2.ArtifactReference) error {
	if response.ContentLength != reference.SizeBytes || response.Header.Get("Content-Type") != reference.MediaType ||
		response.Header.Get(artifactcap.ObjectDigestHeader) != reference.Digest {
		return fmt.Errorf("artifact download response metadata mismatch")
	}
	return nil
}

func drainBounded(reader io.Reader) {
	_, _ = io.Copy(io.Discard, io.LimitReader(reader, maxArtifactClientResponseBytes))
}

func constantArtifactStringEqual(left, right string) bool {
	return len(left) == len(right) && subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}
