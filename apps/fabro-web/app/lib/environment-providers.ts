import type { Environment, ServerSandboxProviderSettings } from "@qltysh/fabro-api-client";

// The providers linked into the server. Any other provider kind names a
// sandbox-driver plugin the operator configured under
// `server.sandbox.providers.<kind>`.
export const LOCAL_PROVIDER = "local";
export const DOCKER_PROVIDER = "docker";
export const DAYTONA_PROVIDER = "daytona";

export const BUNDLED_PROVIDERS = [LOCAL_PROVIDER, DOCKER_PROVIDER, DAYTONA_PROVIDER] as const;

export type ProviderSettingsMap = { [kind: string]: ServerSandboxProviderSettings };

// `local` runs in the caller's directory and never clones. Every other
// provider owns an isolated workspace that Fabro clones into.
export function isCloneBasedProvider(provider: string): boolean {
  return provider !== LOCAL_PROVIDER;
}

// Whether a server-managed environment can back Git-targeted work such as
// automations: only clone-based providers qualify.
export function isCloneBasedEnvironment(environment: Environment): boolean {
  return isCloneBasedProvider(environment.provider);
}

// Providers a managed environment can be created with: every enabled
// clone-based provider. `local` is a reserved, in-memory environment, never a
// managed-environment provider, so it is never offered.
export function creatableProviders(providers: ProviderSettingsMap): string[] {
  return Object.keys(providers)
    .filter((kind) => isCloneBasedProvider(kind) && providers[kind]?.enabled)
    .sort(compareProviderKinds);
}

// Bundled kinds first, in their canonical order, then plugins alphabetically.
export function compareProviderKinds(left: string, right: string): number {
  const rank = (kind: string) => {
    const index = (BUNDLED_PROVIDERS as readonly string[]).indexOf(kind);
    return index === -1 ? BUNDLED_PROVIDERS.length : index;
  };
  return rank(left) - rank(right) || left.localeCompare(right);
}

export function providerLabel(provider: string): string {
  return provider.charAt(0).toUpperCase() + provider.slice(1);
}
