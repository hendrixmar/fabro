import { useId } from "react";
import useSWR, { type SWRConfiguration } from "swr";

import { getInstallSession, type InstallSessionResponse } from "./install-api";

type InstallSessionKey = readonly ["install", "session", string, string];

/**
 * Reads the install session through SWR so server state is owned by the query
 * layer instead of a component effect. Revalidation is explicit because install
 * setup writes refresh the session from their submit path.
 */
export function useInstallSessionQuery(
  token: string | null,
  options: SWRConfiguration<InstallSessionResponse, Error> = {},
) {
  // A GitHub redirect can change the session outside this browser instance.
  // Each installer mount must fetch fresh state instead of reusing an old mount.
  const mountId = useId();
  const key: InstallSessionKey | null = token
    ? ["install", "session", token, mountId]
    : null;
  return useSWR<InstallSessionResponse, Error, InstallSessionKey | null>(
    key,
    ([, , currentToken]) => getInstallSession(currentToken),
    {
      dedupingInterval:       0,
      revalidateOnFocus:      false,
      revalidateOnReconnect:  false,
      shouldRetryOnError:     false,
      ...options,
    },
  );
}
