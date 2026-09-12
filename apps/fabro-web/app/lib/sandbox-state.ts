import type { SandboxState } from "@qltysh/fabro-api-client";

export interface SandboxStateDisplay {
  /** Short human-readable label, e.g. "Running". */
  label: string;
  /** One-sentence explanation shown on hover. */
  description: string;
  /** Tailwind background class for the status dot. */
  dot: string;
  /** Tailwind text color class matching the dot. */
  text: string;
}

const PENDING = { dot: "bg-amber", text: "text-amber" } as const;
const QUIET = { dot: "bg-fg-muted", text: "text-fg-muted" } as const;
const GONE = { dot: "bg-coral", text: "text-coral" } as const;

/**
 * Display metadata for every sandbox driver lifecycle state. Shared by the
 * run overview summary panel and the dedicated sandbox page so the dot color,
 * label, and hover copy stay consistent. A state this build does not know
 * renders as `unknown`.
 */
export const SANDBOX_STATE_DISPLAY: Record<SandboxState, SandboxStateDisplay> = {
  unknown: {
    label: "Unknown",
    description: "The sandbox state could not be determined.",
    ...QUIET,
  },
  creating: {
    label: "Creating",
    description: "The sandbox is being created.",
    ...PENDING,
  },
  starting: {
    label: "Starting",
    description: "The sandbox is starting up.",
    ...PENDING,
  },
  running: {
    label: "Running",
    description: "The sandbox is running.",
    dot: "bg-teal-500",
    text: "text-teal-500",
  },
  stopping: {
    label: "Stopping",
    description: "The sandbox is shutting down.",
    ...PENDING,
  },
  stopped: {
    label: "Stopped",
    description: "The sandbox is stopped.",
    ...QUIET,
  },
  pausing: {
    label: "Pausing",
    description: "The sandbox is being paused.",
    ...PENDING,
  },
  paused: {
    label: "Paused",
    description: "The sandbox is paused.",
    ...PENDING,
  },
  resuming: {
    label: "Resuming",
    description: "The sandbox is resuming.",
    ...PENDING,
  },
  archiving: {
    label: "Archiving",
    description: "The sandbox is being archived.",
    ...PENDING,
  },
  archived: {
    label: "Archived",
    description: "The sandbox has been archived.",
    ...QUIET,
  },
  restoring: {
    label: "Restoring",
    description: "The sandbox is being restored.",
    ...PENDING,
  },
  resizing: {
    label: "Resizing",
    description: "The sandbox resources are being resized.",
    ...PENDING,
  },
  forking: {
    label: "Forking",
    description: "The sandbox is being forked.",
    ...PENDING,
  },
  snapshotting: {
    label: "Snapshotting",
    description: "A snapshot of the sandbox is being taken.",
    ...PENDING,
  },
  deleting: {
    label: "Deleting",
    description: "The sandbox is being deleted.",
    ...PENDING,
  },
  deleted: {
    label: "Deleted",
    description: "The sandbox has been deleted.",
    ...GONE,
  },
  error: {
    label: "Error",
    description: "The sandbox encountered an error.",
    ...GONE,
  },
};
