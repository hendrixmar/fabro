import { afterEach, beforeEach, describe, expect, mock, test } from "bun:test";
import TestRenderer, { act } from "react-test-renderer";
import type { SandboxService } from "@qltysh/fabro-api-client";

import {
  ServicesPanelView,
  type PreviewMutationShape,
  type ServicesQueryShape,
} from "./services-panel";
import { ApiError } from "../../lib/api-client";

function makeIdleQuery(): ServicesQueryShape {
  return {
    data:         undefined,
    error:        undefined,
    isLoading:    false,
    isValidating: false,
    mutate:       () => undefined,
  };
}

function makeIdlePreview(): PreviewMutationShape {
  return {
    trigger: () => Promise.resolve({ intent: "preview", url: "" }),
  };
}

function makeServicesData(data: SandboxService[]) {
  return { data };
}

const mountedRenderers: TestRenderer.ReactTestRenderer[] = [];

interface WindowLike {
  open: (...args: unknown[]) => unknown;
}

const globalRef = globalThis as { window?: WindowLike };
let hadWindow = false;
let originalWindowOpen: WindowLike["open"] | undefined;

function renderView(props: {
  servicesQuery:   ServicesQueryShape;
  previewMutation: PreviewMutationShape;
}): TestRenderer.ReactTestRenderer {
  let renderer!: TestRenderer.ReactTestRenderer;
  act(() => {
    renderer = TestRenderer.create(
      <ServicesPanelView
        runId="run_1"
        servicesQuery={props.servicesQuery}
        previewMutation={props.previewMutation}
      />,
    );
  });
  mountedRenderers.push(renderer);
  return renderer;
}

function installWindowOpen(spy: WindowLike["open"]) {
  if (globalRef.window) {
    hadWindow = true;
    originalWindowOpen = globalRef.window.open;
    globalRef.window.open = spy;
  } else {
    hadWindow = false;
    globalRef.window = { open: spy };
  }
}

beforeEach(() => {
  hadWindow = false;
  originalWindowOpen = undefined;
});

afterEach(() => {
  for (const renderer of mountedRenderers.splice(0)) {
    act(() => renderer.unmount());
  }
  if (hadWindow && globalRef.window && originalWindowOpen) {
    globalRef.window.open = originalWindowOpen;
  } else {
    delete globalRef.window;
  }
});

describe("ServicesPanelView", () => {
  test("shows loading state while fetching with no data", () => {
    const renderer = renderView({
      servicesQuery:   { ...makeIdleQuery(), isLoading: true },
      previewMutation: makeIdlePreview(),
    });
    const labels = renderer.root.findAll(
      (node) =>
        node.type === "p" &&
        Array.isArray(node.children) &&
        node.children.includes("Loading services…"),
    );
    expect(labels).toHaveLength(1);
  });

  test("shows empty state when the services list is empty", () => {
    const renderer = renderView({
      servicesQuery:   { ...makeIdleQuery(), data: makeServicesData([]) },
      previewMutation: makeIdlePreview(),
    });
    const titles = renderer.root.findAll(
      (node) =>
        node.type === "p" &&
        Array.isArray(node.children) &&
        node.children.includes("No services"),
    );
    expect(titles).toHaveLength(1);
  });

  test("shows API error state with the error message", () => {
    const renderer = renderView({
      servicesQuery: {
        ...makeIdleQuery(),
        error: new ApiError({
          status:    500,
          message:   "boom",
          requestId: null,
          body:      null,
        }),
      },
      previewMutation: makeIdlePreview(),
    });
    const descriptions = renderer.root.findAll(
      (node) =>
        node.type === "p" &&
        Array.isArray(node.children) &&
        node.children.includes("boom"),
    );
    expect(descriptions.length).toBeGreaterThan(0);
  });

  test("renders a non-previewable service without a Preview button", () => {
    const service: SandboxService = {
      port:              2500,
      addresses:         ["0.0.0.0:2500"],
      processes:         ["systemd-resolve"],
      preview_supported: false,
    };
    const renderer = renderView({
      servicesQuery:   { ...makeIdleQuery(), data: makeServicesData([service]) },
      previewMutation: makeIdlePreview(),
    });

    const portCells = renderer.root.findAll(
      (node) =>
        node.type === "td" &&
        Array.isArray(node.children) &&
        node.children.includes("2500"),
    );
    expect(portCells).toHaveLength(1);

    const previewButtons = renderer.root.findAll(
      (node) =>
        node.type === "button" &&
        Array.isArray(node.children) &&
        node.children.includes("Preview"),
    );
    expect(previewButtons).toHaveLength(0);
  });

  test("renders a Preview button for a previewable service and triggers with signed args", async () => {
    const service: SandboxService = {
      port:              3000,
      addresses:         ["0.0.0.0:3000"],
      processes:         ["node"],
      preview_supported: true,
    };
    const trigger = mock(() =>
      Promise.resolve({
        intent: "preview" as const,
        url:    "https://preview.example.com/sb-1/3000?sig=abc",
      }),
    );

    const windowOpenSpy = mock(() => null);
    installWindowOpen(windowOpenSpy);

    const renderer = renderView({
      servicesQuery:   { ...makeIdleQuery(), data: makeServicesData([service]) },
      previewMutation: { trigger },
    });
    const previewButton = renderer.root.find(
      (node) =>
        node.type === "button" &&
        Array.isArray(node.children) &&
        node.children.includes("Preview"),
    );

    await act(async () => {
      await previewButton.props.onClick();
    });

    expect(trigger).toHaveBeenCalledTimes(1);
    expect(trigger).toHaveBeenCalledWith({
      port:            3000,
      expires_in_secs: 3600,
      signed:          true,
    });
  });

  test("clicking Preview opens the returned URL in a new tab", async () => {
    const service: SandboxService = {
      port:              3000,
      addresses:         ["0.0.0.0:3000"],
      processes:         ["node"],
      preview_supported: true,
    };
    const trigger = mock(() =>
      Promise.resolve({
        intent: "preview" as const,
        url:    "https://preview.example.com/sb-1/3000?sig=abc",
      }),
    );

    const windowOpenSpy = mock(() => null);
    installWindowOpen(windowOpenSpy);

    const renderer = renderView({
      servicesQuery:   { ...makeIdleQuery(), data: makeServicesData([service]) },
      previewMutation: { trigger },
    });
    const previewButton = renderer.root.find(
      (node) =>
        node.type === "button" &&
        Array.isArray(node.children) &&
        node.children.includes("Preview"),
    );

    await act(async () => {
      await previewButton.props.onClick();
    });

    expect(windowOpenSpy).toHaveBeenCalledTimes(1);
    expect(windowOpenSpy).toHaveBeenCalledWith(
      "https://preview.example.com/sb-1/3000?sig=abc",
      "_blank",
      "noopener,noreferrer",
    );
  });
});
