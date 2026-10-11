// Gateway client authentication for the k6 benchmark.
//
// This module imports nothing from k6/* so Node can load it in tests. File access is injected
// (k6's `open` is only available in the init context).
//
// The settings mirror golang/kvcli: mtls is the default and fails closed; development-plaintext
// must be selected explicitly and only for a local-development deployment (KVDB_ENV).

export const DEVELOPMENT_IDENTITY_HEADER = "x-kvdb-development-identity";

const MODE_MTLS = "mtls";
const MODE_DEVELOPMENT_PLAINTEXT = "development-plaintext";
const DEVELOPMENT_ENVIRONMENTS = new Set(["local", "dev", "development", "test"]);
// The identity header is split on "/", so a tenant or principal must not contain one; keep the
// charset to what is safe in a header value and in the JSON request context.
const IDENTITY_PART = /^[A-Za-z0-9._:@-]+$/;

function trimmed(value) {
  return value === undefined || value === null ? "" : String(value).trim();
}

function identityPart(name, value) {
  if (!IDENTITY_PART.test(value)) {
    throw new Error(`${name} must be non-empty and contain only letters, digits and . _ : @ - (got "${value}")`);
  }
  return value;
}

function readPem(name, path, readFile) {
  if (!path) {
    throw new Error(`${name} is required when KVDB_GRPC_SECURITY_MODE=mtls`);
  }
  let content;
  try {
    content = readFile(path);
  } catch (error) {
    throw new Error(`${name} ${path} is not readable: ${error}`);
  }
  if (!content || !String(content).includes("-----BEGIN")) {
    throw new Error(`${name} ${path} does not contain PEM data`);
  }
  return content;
}

/**
 * Resolves how the benchmark authenticates to the gateway.
 *
 * @param env       environment map (k6 __ENV)
 * @param defaults  { tenant, principal } used in development-plaintext mode when none is configured
 * @param readFile  (path) => string
 * @returns { mode, tenant, principal, metadata, tls }
 *   tls is the k6 connect `tls` option in mtls mode and null in development-plaintext mode.
 */
export function resolveClientAuth(env, defaults, readFile) {
  const mode = trimmed(env.KVDB_GRPC_SECURITY_MODE) || MODE_MTLS;
  const tenant = trimmed(env.KVDB_CLIENT_TENANT_ID);
  const principal = trimmed(env.KVDB_CLIENT_PRINCIPAL);

  if (mode === MODE_DEVELOPMENT_PLAINTEXT) {
    const deployment = trimmed(env.KVDB_ENV).toLowerCase();
    if (!DEVELOPMENT_ENVIRONMENTS.has(deployment)) {
      throw new Error(
        "KVDB_GRPC_SECURITY_MODE=development-plaintext requires KVDB_ENV to be local, dev, development or test"
      );
    }
    const devTenant = identityPart("KVDB_CLIENT_TENANT_ID", tenant || defaults.tenant);
    const devPrincipal = identityPart("KVDB_CLIENT_PRINCIPAL", principal || defaults.principal);
    return {
      mode,
      tenant: devTenant,
      principal: devPrincipal,
      metadata: { [DEVELOPMENT_IDENTITY_HEADER]: `client/${devTenant}/${devPrincipal}` },
      tls: null,
    };
  }

  if (mode === MODE_MTLS) {
    // The verified identity comes from the client certificate SAN. A request context claim must
    // agree with it, so none is sent unless the operator configured one.
    const tls = {
      cacerts: [readPem("KVDB_CLIENT_TLS_TRUST_BUNDLE", trimmed(env.KVDB_CLIENT_TLS_TRUST_BUNDLE), readFile)],
      cert: readPem("KVDB_CLIENT_TLS_CERT_CHAIN", trimmed(env.KVDB_CLIENT_TLS_CERT_CHAIN), readFile),
      key: readPem("KVDB_CLIENT_TLS_PRIVATE_KEY", trimmed(env.KVDB_CLIENT_TLS_PRIVATE_KEY), readFile),
    };
    return {
      mode,
      tenant: tenant ? identityPart("KVDB_CLIENT_TENANT_ID", tenant) : "",
      principal: principal ? identityPart("KVDB_CLIENT_PRINCIPAL", principal) : "",
      metadata: {},
      tls,
    };
  }

  throw new Error(`KVDB_GRPC_SECURITY_MODE must be mtls or development-plaintext (got "${mode}")`);
}

/** Options for k6 grpc `client.connect`; plaintext is only ever set in development-plaintext mode. */
export function buildConnectParams(auth) {
  return auth.mode === MODE_DEVELOPMENT_PLAINTEXT ? { plaintext: true } : { tls: auth.tls };
}

/** Options for k6 grpc `client.invoke`. */
export function buildInvokeParams(auth, timeoutMs) {
  return { timeout: timeoutMs, metadata: { ...auth.metadata } };
}

/** RequestContext whose tenant/principal agree with the transport identity. */
export function buildRequestContext(auth, requestId) {
  const ctx = { request_id: requestId, traceparent: "" };
  if (auth.tenant) ctx.tenant_id = auth.tenant;
  if (auth.principal) ctx.principal = auth.principal;
  return ctx;
}
