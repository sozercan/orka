import React from 'react';
import CodeBlock from '@theme/CodeBlock';
import Link from '@docusaurus/Link';

export default function QuickStartSection() {
  return (
    <section className="landing-section quickstart-section">
      <h2 className="section-title">Quick start</h2>
      <p className="section-subtitle">
        Install the release, then complete API access and provider setup with
        the getting-started guide.
      </p>
      <div className="quickstart-grid">
        <div className="quickstart-card">
          <h3>Install the controller</h3>
          <p>
            CRDs, RBAC, controller, and the built-in dashboard. The manifest
            mounts a harness-wrapper-auth Secret without creating it, so make
            the namespace and that Secret first.
          </p>
          <CodeBlock language="bash">{`kubectl create namespace orka-system
kubectl -n orka-system create secret generic harness-wrapper-auth \\
  --from-literal=token="$(openssl rand -hex 32)"

kubectl apply -f https://raw.githubusercontent.com/orka-agents/orka/v0.1.3/deploy/orka.yaml

kubectl -n orka-system rollout status deploy/orka-controller-manager`}</CodeBlock>
        </div>
        <div className="quickstart-card">
          <h3>Configure access and open the dashboard</h3>
          <p>
            Complete the{' '}
            <Link to="/docs/getting-started#give-yourself-an-api-client">
              API client setup
            </Link>{' '}
            and{' '}
            <Link to="/docs/getting-started#your-first-task">Provider setup</Link>{' '}
            in Getting started. Then forward the API port and sign in to the
            dashboard with your client token.
          </p>
          <CodeBlock language="bash">{`kubectl port-forward -n orka-system svc/orka-api 8080:8080
# open http://localhost:8080`}</CodeBlock>
        </div>
      </div>
      <p className="section-subtitle">
        Running coding agents such as Codex or Claude Code on the ACP runtime path
        needs a build from{' '}
        <code>main</code> — see{' '}
        <Link to="/docs/getting-started">Getting started</Link> and{' '}
        <Link to="/docs/release-status">Release status</Link>.
      </p>
    </section>
  );
}
