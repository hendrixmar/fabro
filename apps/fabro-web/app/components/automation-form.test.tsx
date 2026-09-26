import { describe, expect, test } from "bun:test";
import TestRenderer, { act } from "react-test-renderer";
import { MemoryRouter } from "react-router";

import {
  AutomationFormFields,
  EMPTY_AUTOMATION_FORM,
  automationFormValuesFromRun,
  automationToFormValues,
  isFormValid,
  workflowSourceFromFormValues,
  type AutomationFormValues,
} from "./automation-form";

const defaultValues: AutomationFormValues = {
  ...EMPTY_AUTOMATION_FORM,
  id:               "nightly",
  name:             "Nightly",
  environmentId:    "daytona-smoke",
  targetRepository: "fabro-sh/app",
  targetBranch:     "main",
  workflow:         "release",
};

/** Render `AutomationFormFields` and flatten it to its visible text. */
function renderForm(values: AutomationFormValues): string {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  let tree!: TestRenderer.ReactTestRenderer;
  act(() => {
    tree = TestRenderer.create(
      <MemoryRouter>
        <AutomationFormFields values={values} onChange={() => {}} />
      </MemoryRouter>,
    );
  });
  return textOf(tree.toJSON());
}

function textOf(node: unknown): string {
  if (!node) return "";
  if (typeof node === "string") return node;
  if (Array.isArray(node)) return node.map(textOf).join("");
  const element = node as { children?: unknown[] };
  return (element.children ?? []).map(textOf).join("");
}

describe("automation workflow source form values", () => {
  test("the default and create-from-run forms inherit the target checkout", () => {
    expect(workflowSourceFromFormValues(EMPTY_AUTOMATION_FORM)).toBeUndefined();

    const values = automationFormValuesFromRun({
      title: "Release",
      workflow: { name: "Release", graph_name: "release", slug: "release" },
      repository: {
        name:       "fabro-sh/fabro",
        origin_url: "https://github.com/fabro-sh/fabro.git",
      },
      sandbox: null,
    } as any);
    expect(values.usesRemoteWorkflow).toBe(false);
    expect(workflowSourceFromFormValues(values)).toBeUndefined();
  });

  test("branch, tag, and SHA selectors serialize with target precedence", () => {
    const base = {
      ...EMPTY_AUTOMATION_FORM,
      usesRemoteWorkflow:         true,
      workflowSourceRepository: " fabro-sh/workflows ",
      workflowSourceBranch:     " main ",
    };

    expect(workflowSourceFromFormValues(base)).toEqual({
      repo: "fabro-sh/workflows", branch: "main",
    });
    expect(workflowSourceFromFormValues({
      ...base,
      workflowSourceTag: " v1.2.3 ",
    })).toEqual({ repo: "fabro-sh/workflows", branch: "main", tag: "v1.2.3" });
    expect(workflowSourceFromFormValues({
      ...base,
      workflowSourceTag: " v1.2.3 ",
      workflowSourceSha: "ABCDEF0123456789ABCDEF0123456789ABCDEF01",
    })).toEqual({
      repo: "fabro-sh/workflows",
      branch: "main",
      tag: "v1.2.3",
      sha: "abcdef0123456789abcdef0123456789abcdef01",
    });
  });

  test("remote workflow fields are required and SHAs need 40 hex characters", () => {
    const validBase = {
      ...EMPTY_AUTOMATION_FORM,
      id:                       "nightly",
      name:                     "Nightly",
      environmentId:            "daytona-smoke",
      targetRepository:         "fabro-sh/app",
      targetBranch:             "main",
      workflow:                 "release",
      usesRemoteWorkflow:         true,
      workflowSourceRepository: "fabro-sh/workflows",
      workflowSourceBranch:     "main",
      workflowSourceSha:        "0123456789abcdef0123456789abcdef01234567",
    };

    expect(isFormValid(validBase)).toBe(true);
    expect(isFormValid({ ...validBase, workflowSourceRepository: "" })).toBe(false);
    expect(isFormValid({ ...validBase, workflowSourceBranch: "" })).toBe(false);
    expect(isFormValid({ ...validBase, workflowSourceSha: "short" })).toBe(false);
  });

  test("editing preserves an explicit source even when it equals the target", () => {
    const values = automationToFormValues({
      id:          "nightly",
      revision:    "revision",
      name:        "Nightly",
      description: null,
      target:      { kind: "git", repo: "fabro-sh/fabro", branch: "main" },
      workflow:    "release",
      workflow_source: { repo: "fabro-sh/fabro", branch: "main" },
      triggers:    [],
    });

    expect(values.usesRemoteWorkflow).toBe(true);
    expect(values.workflowSourceRepository).toBe("fabro-sh/fabro");
    expect(values.workflowSourceBranch).toBe("main");
    expect(values.workflowSourceTag).toBe("");
    expect(values.workflowSourceSha).toBe("");
    expect(workflowSourceFromFormValues({
      ...values,
      usesRemoteWorkflow: false,
    })).toBeUndefined();
  });

  test("a linked automation shows its global workflow read-only", () => {
    const text = renderForm({ ...defaultValues, sourceAutomationId: "woodpecker-loop", workflow: "woodpecker-loop" });
    expect(text).toContain("Runs the global woodpecker-loop workflow");
    expect(text).not.toContain("Workflow slug");
  });
});
