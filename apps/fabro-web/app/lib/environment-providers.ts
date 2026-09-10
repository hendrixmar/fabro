import { EnvironmentProvider, type Environment } from "@qltysh/fabro-api-client";

// Providers a managed environment can be created with. `local` is a reserved,
// in-memory environment, never a managed-environment provider, so it is never
// offered. The provider is fixed at creation time and cannot be changed.
export const CREATABLE_PROVIDERS = [
  EnvironmentProvider.DOCKER,
  EnvironmentProvider.DAYTONA,
] as const;

// Whether a server-managed environment can back Git-targeted work such as
// automations: only the clone-based (creatable) providers qualify.
export function isCloneBasedEnvironment(environment: Environment): boolean {
  return (CREATABLE_PROVIDERS as readonly string[]).includes(environment.provider);
}

export function providerLabel(provider: string): string {
  return provider.charAt(0).toUpperCase() + provider.slice(1);
}
