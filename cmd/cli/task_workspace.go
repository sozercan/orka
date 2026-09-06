package main

import (
	"fmt"
	"strings"
	"text/tabwriter"
	"unicode/utf8"

	"github.com/spf13/cobra"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/cli/client"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
)

type taskWorkspaceCreateOptions struct {
	intent                        string
	gitRepo                       string
	sourceRepositoryProvider      string
	sourceRepositoryID            string
	branch                        string
	ref                           string
	subPath                       string
	readCredential                string
	readCredentialKey             string
	publicationGitRepo            string
	publicationRepositoryProvider string
	publicationRepositoryID       string
	publicationReadCredential     string
	publicationReadCredentialKey  string
	publicationCredential         string
	publicationCredentialKey      string
	forgeCredential               string
	forgeCredentialKey            string
	pushBranch                    string
	prBaseBranch                  string
	createPR                      bool
}

func (o *taskWorkspaceCreateOptions) bindFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.intent, "workspace-intent", "read", "Agent workspace intent: read or write")
	cmd.Flags().StringVar(&o.gitRepo, "git-repo", "", "Source repository URL (credentials must not be embedded)")
	cmd.Flags().StringVar(&o.sourceRepositoryProvider, "source-repository-provider", "", "Canonical source repository provider")
	cmd.Flags().StringVar(&o.sourceRepositoryID, "source-repository-id", "", "Canonical source repository ID")
	cmd.Flags().StringVar(&o.branch, "branch", "", "Source branch")
	cmd.Flags().StringVar(&o.ref, "ref", "", "Source commit, tag, or ref")
	cmd.Flags().StringVar(&o.subPath, "sub-path", "", "Subdirectory within the source repository")
	cmd.Flags().StringVar(&o.readCredential, "read-credential", "", "Secret name for source clone/read credentials")
	cmd.Flags().StringVar(&o.readCredentialKey, "read-credential-key", "", "Secret key for source clone/read credentials (default: token)")
	cmd.Flags().StringVar(&o.publicationGitRepo, "publication-git-repo", "", "Publication repository URL")
	cmd.Flags().StringVar(&o.publicationRepositoryProvider, "publication-repository-provider", "", "Canonical publication repository provider")
	cmd.Flags().StringVar(&o.publicationRepositoryID, "publication-repository-id", "", "Canonical publication repository ID")
	cmd.Flags().StringVar(&o.publicationReadCredential, "publication-read-credential", "", "Secret name for publication preflight and verification credentials")
	cmd.Flags().StringVar(&o.publicationReadCredentialKey, "publication-read-credential-key", "", "Secret key for publication preflight and verification credentials (default: token)")
	cmd.Flags().StringVar(&o.publicationCredential, "publication-credential", "", "Secret name for publication write credentials")
	cmd.Flags().StringVar(&o.publicationCredentialKey, "publication-credential-key", "", "Secret key for publication write credentials (default: token)")
	cmd.Flags().StringVar(&o.forgeCredential, "forge-credential", "", "Secret name for forge API credentials used to reconcile pull requests")
	cmd.Flags().StringVar(&o.forgeCredentialKey, "forge-credential-key", "", "Secret key for forge API credentials (default: token)")
	cmd.Flags().StringVar(&o.pushBranch, "push-branch", "", "Publication branch (default: controller-derived full-entropy branch)")
	cmd.Flags().StringVar(&o.prBaseBranch, "pr-base-branch", "", "Pull request base branch")
	cmd.Flags().BoolVar(&o.createPR, "create-pr", false, "Reconcile a pull request after verified publication")
}

func (o taskWorkspaceCreateOptions) build(cmd *cobra.Command, taskType string) (map[string]any, error) {
	intent := strings.ToLower(strings.TrimSpace(o.intent))
	if intent != string(corev1alpha1.WorkspaceIntentRead) && intent != string(corev1alpha1.WorkspaceIntentWrite) {
		return nil, fmt.Errorf("--workspace-intent must be read or write")
	}
	intentFlagUsed, otherWorkspaceFlagsUsed := workspaceFlagUsage(cmd)
	if taskType != cliTaskTypeAgent {
		if otherWorkspaceFlagsUsed || intentFlagUsed {
			return nil, fmt.Errorf("workspace flags are supported only for agent tasks")
		}
		return nil, nil
	}
	// Only serialize a workspace when the user actually configured a workspace
	// field: a bare {intent: "read"} — whether defaulted or passed explicitly —
	// would make an otherwise valid prompt-only agent Task fail preflight in
	// harness-v1 mode, which requires gitRepo on any non-nil workspace. An
	// explicit write intent is a real configuration and proceeds so its
	// missing-gitRepo validation error surfaces instead of being dropped.
	if !otherWorkspaceFlagsUsed && (!intentFlagUsed || intent != string(corev1alpha1.WorkspaceIntentWrite)) {
		return nil, nil
	}
	if err := o.canonicalizeRepositoryURLs(); err != nil {
		return nil, err
	}
	if err := o.validateWorkspaceFlags(); err != nil {
		return nil, err
	}
	publicationRequested := o.createPR || strings.TrimSpace(o.publicationGitRepo) != "" ||
		strings.TrimSpace(o.publicationRepositoryProvider) != "" || strings.TrimSpace(o.publicationReadCredential) != "" ||
		strings.TrimSpace(o.publicationReadCredentialKey) != "" || strings.TrimSpace(o.publicationCredential) != "" ||
		strings.TrimSpace(o.publicationCredentialKey) != "" || strings.TrimSpace(o.forgeCredential) != "" ||
		strings.TrimSpace(o.forgeCredentialKey) != "" || strings.TrimSpace(o.pushBranch) != "" || strings.TrimSpace(o.prBaseBranch) != ""
	if err := o.validatePublicationOptions(intent, publicationRequested); err != nil {
		return nil, err
	}

	workspace := map[string]any{"intent": intent}
	addTrimmed(workspace, "gitRepo", o.gitRepo)
	addRepositoryIdentity(workspace, "sourceRepository", o.sourceRepositoryProvider, o.sourceRepositoryID)
	addTrimmed(workspace, "branch", o.branch)
	addTrimmed(workspace, "ref", o.ref)
	addTrimmed(workspace, "subPath", o.subPath)
	addCredentialRef(workspace, "readCredentialRef", o.readCredential, o.readCredentialKey)
	if intent == string(corev1alpha1.WorkspaceIntentWrite) {
		addTrimmed(workspace, "publicationGitRepo", o.publicationGitRepo)
		addRepositoryIdentity(workspace, "publicationRepository", o.publicationRepositoryProvider, o.publicationRepositoryID)
		addCredentialRef(workspace, "publicationReadCredentialRef", o.publicationReadCredential, o.publicationReadCredentialKey)
		addCredentialRef(workspace, "publicationCredentialRef", o.publicationCredential, o.publicationCredentialKey)
		addCredentialRef(workspace, "forgeCredentialRef", o.forgeCredential, o.forgeCredentialKey)
		addTrimmed(workspace, "pushBranch", o.pushBranch)
		addTrimmed(workspace, "prBaseBranch", o.prBaseBranch)
		if o.createPR {
			workspace["createPR"] = true
		}
	}
	return workspace, nil
}

// workspaceFlagUsage reports whether the workspace-intent flag and any other
// workspace flag were explicitly set on the command line.
func workspaceFlagUsage(cmd *cobra.Command) (intentUsed, othersUsed bool) {
	intentUsed = cmd.Flags().Changed("workspace-intent")
	for _, name := range []string{
		"git-repo", "source-repository-provider", "source-repository-id", "branch", "ref",
		"sub-path", "read-credential", "read-credential-key", "publication-git-repo", "publication-repository-provider",
		"publication-repository-id", "publication-read-credential", "publication-read-credential-key",
		"publication-credential", "publication-credential-key", "forge-credential", "forge-credential-key",
		"push-branch", "pr-base-branch", "create-pr",
	} {
		othersUsed = othersUsed || cmd.Flags().Changed(name)
	}
	return intentUsed, othersUsed
}

// validateWorkspaceFlags runs the full preflight mirror over the workspace
// flags: source-selector dependencies, canonical source refs, paired identity
// flags, canonical repository identities, publication branches, and
// credential key/name pairing.
func (o taskWorkspaceCreateOptions) validateWorkspaceFlags() error {
	if err := o.validateSourceSelectorDependencies(); err != nil {
		return err
	}
	if err := o.validateSourceSelectors(); err != nil {
		return err
	}
	if (strings.TrimSpace(o.sourceRepositoryProvider) == "") != (strings.TrimSpace(o.sourceRepositoryID) == "") {
		return fmt.Errorf("--source-repository-provider and --source-repository-id must be set together")
	}
	if (strings.TrimSpace(o.publicationRepositoryProvider) == "") != (strings.TrimSpace(o.publicationRepositoryID) == "") {
		return fmt.Errorf("--publication-repository-provider and --publication-repository-id must be set together")
	}
	if err := o.validateRepositoryIdentities(); err != nil {
		return err
	}
	if err := o.validatePublicationBranches(); err != nil {
		return err
	}
	if err := validateWorkspaceSubPathFlag(o.subPath); err != nil {
		return err
	}
	for _, credential := range []struct {
		nameFlag string
		name     string
		keyFlag  string
		key      string
	}{
		{nameFlag: "--read-credential", name: o.readCredential, keyFlag: "--read-credential-key", key: o.readCredentialKey},
		{nameFlag: "--publication-read-credential", name: o.publicationReadCredential, keyFlag: "--publication-read-credential-key", key: o.publicationReadCredentialKey},
		{nameFlag: "--publication-credential", name: o.publicationCredential, keyFlag: "--publication-credential-key", key: o.publicationCredentialKey},
		{nameFlag: "--forge-credential", name: o.forgeCredential, keyFlag: "--forge-credential-key", key: o.forgeCredentialKey},
	} {
		if strings.TrimSpace(credential.key) != "" && strings.TrimSpace(credential.name) == "" {
			return fmt.Errorf("%s requires %s", credential.keyFlag, credential.nameFlag)
		}
	}
	return nil
}

// validateWorkspaceSubPathFlag mirrors the harness-v2 workspace relative-root
// validation so an unsafe --sub-path fails at create time instead of failing
// RuntimeSession creation.
func validateWorkspaceSubPathFlag(subPath string) error {
	root := strings.TrimSpace(subPath)
	if root == "" || root == "." {
		return nil
	}
	invalid := func(detail string) error {
		return fmt.Errorf("--sub-path %s (use a relative slash-separated path inside the repository)", detail)
	}
	if !utf8.ValidString(root) {
		return invalid("contains invalid UTF-8")
	}
	if len(root) > 1024 {
		return invalid("exceeds 1024 bytes")
	}
	if strings.HasPrefix(root, "/") || strings.Contains(root, `\`) {
		return invalid("must be a relative slash-separated path")
	}
	for segment := range strings.SplitSeq(root, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return invalid("contains an unsafe segment")
		}
	}
	return nil
}

// validateSourceSelectorDependencies mirrors the controller workspace
// preflight rule that source selectors and read credentials require a
// repository, so a doomed Task fails here instead of after creation.
func (o taskWorkspaceCreateOptions) validateSourceSelectorDependencies() error {
	if strings.TrimSpace(o.gitRepo) != "" {
		return nil
	}
	for _, dependent := range []struct {
		flag  string
		value string
	}{
		{flag: "--branch", value: o.branch},
		{flag: "--ref", value: o.ref},
		{flag: "--sub-path", value: o.subPath},
		{flag: "--read-credential", value: o.readCredential},
		{flag: "--source-repository-provider", value: o.sourceRepositoryProvider},
		{flag: "--source-repository-id", value: o.sourceRepositoryID},
	} {
		if strings.TrimSpace(dependent.value) != "" {
			return fmt.Errorf("%s requires --git-repo", dependent.flag)
		}
	}
	return nil
}

// validateSourceSelectors mirrors the controller's runtimeWorkspaceSourceRef
// selector validation with the same canonical source-ref validator, so a Task
// with a malformed branch or ref selector fails here instead of after
// creation. Keep in exact behavior parity with the controller.
func (o taskWorkspaceCreateOptions) validateSourceSelectors() error {
	if ref := strings.TrimSpace(o.ref); ref != "" {
		if _, err := publisherservice.CanonicalWorkspaceSourceRef(ref); err != nil {
			return fmt.Errorf("--ref is invalid: %v", err)
		}
	}
	if branch := strings.TrimSpace(o.branch); branch != "" {
		candidate := branch
		if !strings.HasPrefix(candidate, "refs/") {
			candidate = "refs/heads/" + candidate
		}
		if _, err := publisherservice.CanonicalWorkspaceSourceRef(candidate); err != nil {
			return fmt.Errorf("--branch is invalid: %v", err)
		}
	}
	return nil
}

// validateRepositoryIdentities mirrors the controller's canonical repository
// identity checks (workspaceRepository / workspacePublicationRepository): a
// supplied repository identity must use the github provider and match the
// canonical credential-free URL identity, with the publication identity
// derived from --publication-git-repo or falling back to --git-repo.
func (o taskWorkspaceCreateOptions) validateRepositoryIdentities() error {
	if strings.TrimSpace(o.sourceRepositoryProvider) != "" || strings.TrimSpace(o.sourceRepositoryID) != "" {
		if err := validateRepositoryIdentityAgainstURL("--source-repository", o.sourceRepositoryProvider, o.sourceRepositoryID, o.gitRepo); err != nil {
			return err
		}
	}
	if strings.TrimSpace(o.publicationRepositoryProvider) != "" || strings.TrimSpace(o.publicationRepositoryID) != "" {
		publicationURL := strings.TrimSpace(o.publicationGitRepo)
		if publicationURL == "" {
			publicationURL = strings.TrimSpace(o.gitRepo)
		}
		if err := validateRepositoryIdentityAgainstURL("--publication-repository", o.publicationRepositoryProvider, o.publicationRepositoryID, publicationURL); err != nil {
			return err
		}
	}
	return nil
}

func validateRepositoryIdentityAgainstURL(flagPrefix, provider, id, canonicalURL string) error {
	if strings.ToLower(strings.TrimSpace(provider)) != "github" {
		return fmt.Errorf("%s-provider must be github", flagPrefix)
	}
	derived, err := security.WorkspaceRepositoryURLIdentity(canonicalURL)
	if err != nil {
		return fmt.Errorf("%s-id requires a valid repository URL: %v", flagPrefix, err)
	}
	if !security.SameWorkspaceRepositoryIdentity(strings.TrimSpace(id), derived) {
		return fmt.Errorf("%s-id must match the canonical credential-free URL identity %q", flagPrefix, derived)
	}
	return nil
}

// validatePublicationBranches mirrors the controller's
// canonicalWorkspaceBranchRef validation for publication branch flags.
func (o taskWorkspaceCreateOptions) validatePublicationBranches() error {
	for _, branch := range []struct {
		flag  string
		value string
	}{
		{flag: "--push-branch", value: o.pushBranch},
		{flag: "--pr-base-branch", value: o.prBaseBranch},
	} {
		value := strings.TrimSpace(branch.value)
		if value == "" {
			continue
		}
		ref := value
		if !strings.HasPrefix(ref, "refs/heads/") {
			ref = "refs/heads/" + ref
		}
		if err := store.ValidateFullBranchRef(ref); err != nil {
			return fmt.Errorf("%s is invalid: %v", branch.flag, err)
		}
	}
	return nil
}

// canonicalizeRepositoryURLs canonicalizes repository URL flags to the only
// form the controller's workspace preflight accepts so a doomed Task fails
// here instead of after creation: GitHub SSH roots are converted
// automatically, everything else must be a credential-free HTTPS URL.
func (o *taskWorkspaceCreateOptions) canonicalizeRepositoryURLs() error {
	for _, field := range []struct {
		flag  string
		value *string
	}{
		{flag: "--git-repo", value: &o.gitRepo},
		{flag: "--publication-git-repo", value: &o.publicationGitRepo},
	} {
		canonical, err := security.CanonicalWorkspaceRepositoryCloneURL(*field.value)
		if err != nil {
			return fmt.Errorf("%s %v (use a credential-free HTTPS URL such as https://github.com/owner/repo; GitHub SSH roots like git@github.com:owner/repo are converted automatically)", field.flag, err)
		}
		*field.value = canonical
	}
	return nil
}

func (o taskWorkspaceCreateOptions) validatePublicationOptions(intent string, publicationRequested bool) error {
	if intent != string(corev1alpha1.WorkspaceIntentWrite) {
		if publicationRequested {
			return fmt.Errorf("publication flags require --workspace-intent write")
		}
		return nil
	}
	if strings.TrimSpace(o.gitRepo) == "" {
		return fmt.Errorf("--workspace-intent write requires --git-repo")
	}
	if strings.TrimSpace(o.publicationCredential) == "" {
		return fmt.Errorf("--workspace-intent write requires --publication-credential")
	}
	if o.createPR && strings.TrimSpace(o.prBaseBranch) == "" {
		return fmt.Errorf("--create-pr requires --pr-base-branch")
	}
	if o.createPR && strings.TrimSpace(o.forgeCredential) == "" {
		return fmt.Errorf("--create-pr requires --forge-credential")
	}
	return nil
}

func addTrimmed(target map[string]any, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		target[key] = value
	}
}

func addRepositoryIdentity(target map[string]any, key, provider, id string) {
	provider = strings.TrimSpace(provider)
	id = strings.TrimSpace(id)
	if provider != "" && id != "" {
		target[key] = map[string]any{"provider": provider, "id": id}
	}
}

func addCredentialRef(target map[string]any, field, name, secretKey string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	ref := map[string]any{"name": name}
	if secretKey = strings.TrimSpace(secretKey); secretKey != "" {
		ref["key"] = secretKey
	}
	target[field] = ref
}

func newTaskRuntimeStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status <name>",
		Short: "Show durable execution, delivery, and runtime-pool status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			detail, err := c.GetTask(cmd.Context(), args[0], client.GetOptions{Namespace: c.Namespace})
			if err != nil {
				return err
			}
			status := safeTaskRuntimeStatus(*detail)
			format, err := outputFormat(cmd)
			if err != nil {
				return err
			}
			if format != outputTable {
				return printStructured(cmd, status)
			}
			return printTaskRuntimeStatusTable(cmd, status)
		},
	}
	addOutputFlag(cmd, outputTable)
	return cmd
}

func safeTaskRuntimeStatus(task client.TaskDetail) map[string]any {
	out := map[string]any{
		"task":      client.StringField(task, "metadata", "name"),
		"namespace": client.StringField(task, "metadata", "namespace"),
	}
	status := nestedMap(task, "status")
	out["phase"] = status["phase"]
	if execution := nestedMap(status, "execution"); len(execution) > 0 {
		out["execution"] = copyKeys(execution,
			"state", "outcome", "reason", "attempt", "promptID", "runtimePoolName", "runtimePoolUID",
			"runtimeInstanceID", "runtimeSessionUID", "runtimeSessionGeneration", "requestDigest", "controllerEpoch",
			"message", "lastTransitionTime")
	}
	if delivery := nestedMap(status, "delivery"); len(delivery) > 0 {
		out["delivery"] = copyKeys(delivery,
			"state", "outcome", "reason", "publicationID", "sourceRepository", "publicationRepository", "branch",
			"startingSHA", "remoteBeforeSHA", "treeSHA", "expectedCommitSHA", "verifiedRemoteSHA", "supersedingRemoteSHA",
			"artifactDigest", "prReceipt", "message", "lastTransitionTime")
	}
	return out
}

func printTaskRuntimeStatusTable(cmd *cobra.Command, status map[string]any) error {
	execution := nestedMap(status, "execution")
	delivery := nestedMap(status, "delivery")
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "FIELD\tVALUE") //nolint:errcheck
	rows := [][2]string{
		{"Task", anyString(status["task"])},
		{"Namespace", anyString(status["namespace"])},
		{"Phase", anyString(status["phase"])},
		{"Execution", anyString(execution["state"])},
		{"Execution outcome", anyString(execution["outcome"])},
		{"Execution reason", anyString(execution["reason"])},
		{"Attempt", anyString(execution["attempt"])},
		{"RuntimePool", anyString(execution["runtimePoolName"])},
		{"Runtime instance", compactCLIValue(anyString(execution["runtimeInstanceID"]))},
		{"Runtime session generation", anyString(execution["runtimeSessionGeneration"])},
		{"Delivery", anyString(delivery["state"])},
		{"Delivery outcome", anyString(delivery["outcome"])},
		{"Delivery message", anyString(delivery["message"])},
		{"Publication branch", anyString(delivery["branch"])},
		{"Verified remote", compactCLIValue(anyString(delivery["verifiedRemoteSHA"]))},
	}
	for _, row := range rows {
		fmt.Fprintf(w, "%s\t%s\n", row[0], dash(row[1])) //nolint:errcheck
	}
	if execution["state"] == "OutcomeUnknown" || execution["outcome"] == "OutcomeUnknown" {
		fmt.Fprintln(w, "Replay policy\tTerminal; create a new Task explicitly. No automatic replay.") //nolint:errcheck
	}
	return w.Flush()
}

func copyKeys(source map[string]any, keys ...string) map[string]any {
	out := map[string]any{}
	for _, key := range keys {
		if value, ok := source[key]; ok {
			out[key] = value
		}
	}
	return out
}
