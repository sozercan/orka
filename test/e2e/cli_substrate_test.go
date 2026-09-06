//go:build e2e
// +build e2e

/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orka CLI substrate pool binary workflow", Ordered, func() {
	var (
		apiBaseURL     string
		portForwardCmd *exec.Cmd
		cancelPF       context.CancelFunc
		token          string
		home           string
		suffix         string
	)

	BeforeAll(func() {
		By("building the orka CLI binary")
		buildOrkaCLI()

		By("setting up a controller API port-forward for substrate CLI commands")
		var err error
		apiBaseURL, cancelPF, portForwardCmd, err = startControllerAPIPortForward(18114)
		Expect(err).NotTo(HaveOccurred(), "Failed to start controller API port-forward")

		By("creating an isolated CLI home with service-account credentials")
		token, err = serviceAccountToken()
		Expect(err).NotTo(HaveOccurred())
		Expect(token).NotTo(BeEmpty())
		home = newIsolatedCLIHome(apiBaseURL, token)
		suffix = fmt.Sprintf("%d", time.Now().UnixNano())
	})

	AfterAll(func() {
		By("stopping substrate CLI controller API port-forward")
		stopPortForward(cancelPF, portForwardCmd)
	})

	It("creates, reads, lists, updates, and deletes a substrate actor pool", func() {
		poolName := "e2e-cli-substrate-pool-" + suffix
		tmpDir := GinkgoT().TempDir()
		DeferCleanup(deleteK8sResource, "substrateactorpool", poolName)

		By("creating a minimal SubstrateActorPool with a templateRef and desired counts")
		poolManifest := writeTempManifest(tmpDir, "pool.yaml", substratePoolManifest(poolName, "e2e-cli-template", 1, false))
		expectOrkaSuccess(runOrka(home, "substrate", "pool", "create", "-f", poolManifest), token)

		By("getting the substrate actor pool as JSON")
		get := runOrka(home, "substrate", "pool", "get", poolName, "-o", "json")
		expectOrkaSuccess(get, token)
		pool := expectJSONObject(get.Stdout)
		Expect(nestedStringFromMap(pool, "metadata", "name")).To(Equal(poolName))
		Expect(nestedStringFromMap(pool, "metadata", "namespace")).To(Equal(namespace))
		Expect(nestedStringFromMap(pool, "spec", "templateRef", "name")).To(Equal("e2e-cli-template"))
		Expect(nestedNumberFromMap(pool, "spec", "targetActors")).To(BeNumerically("==", 1))

		By("listing substrate actor pools as JSON")
		list := runOrka(home, "substrate", "pool", "list", "-o", "json")
		expectOrkaSuccess(list, token)
		expectListContainsName(expectJSONOutput(list.Stdout), poolName)

		By("updating an observable substrate actor pool spec field")
		updatedManifest := writeTempManifest(tmpDir, "pool-updated.yaml", substratePoolManifest(poolName, "e2e-cli-template", 2, true))
		expectOrkaSuccess(runOrka(home, "substrate", "pool", "update", poolName, "-f", updatedManifest), token)
		updated := expectJSONObject(runSuccessfulOrka(home, []string{token}, "substrate", "pool", "get", poolName, "-o", "json").Stdout)
		Expect(nestedNumberFromMap(updated, "spec", "targetActors")).To(BeNumerically("==", 2))
		Expect(nestedBoolFromMap(updated, "spec", "precreateActors")).To(BeTrue())

		By("deleting the substrate actor pool and proving absence")
		expectOrkaSuccess(runOrka(home, "substrate", "pool", "delete", poolName), token)
		missing := runOrka(home, "substrate", "pool", "get", poolName, "-o", "json")
		expectOrkaFailure(missing, token)
		listAfterDelete := runOrka(home, "substrate", "pool", "list", "-o", "json")
		expectOrkaSuccess(listAfterDelete, token)
		expectListDoesNotContainName(expectJSONOutput(listAfterDelete.Stdout), poolName)
	})
})

// substratePoolManifest is minimal for the current SubstrateActorPoolSpec: templateRef is required;
// targetActors/precreateActors provide observable spec fields for create/update assertions.
func substratePoolManifest(name, templateName string, targetActors int32, precreateActors bool) string {
	return fmt.Sprintf(`
apiVersion: core.orka.ai/v1alpha1
kind: SubstrateActorPool
metadata:
  name: %s
spec:
  templateRef:
    name: %s
  targetActors: %d
  precreateActors: %t
`, name, templateName, targetActors, precreateActors)
}
