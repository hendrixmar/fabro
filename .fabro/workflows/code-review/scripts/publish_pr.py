#!/usr/bin/env python3
"""Deterministic PR publisher for the Fabro code-review workflow.

Posts a completed review's findings to the reviewed GitHub PR in two steps:

- ``plan`` is pure: canonical bundle + git diff arithmetic + routing
  configuration -> a publication plan (JSON). No network, no credentials.
  Every placement decision, comment body, batch, and summary body is in the
  plan and is byte-deterministic.
- ``apply`` executes a plan against the GitHub API and writes an outcome
  file. All environmental nondeterminism (HTTP failures, retries, existing
  PR state) is confined here. The plan file is untrusted input: apply
  re-validates it before any write.

The canonical ``lithoscomputer/code-review`` source repository keeps the
requirements register at ``.ai/plans/p1-pr-publisher-requirements.md`` and
the executable specification at ``tests/test_pr_publisher.py``. Packaged
workflow installs do not need to copy those development files. R-numbers
below refer to that canonical requirements register.

Python 3.9-compatible. Standard library only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, NoReturn, Optional, Sequence, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

import render_report as renderer  # noqa: E402  (the bundle validators)
from review_contract import FINDING_ID_RE  # noqa: E402


PLAN_VERSION = 1
SUMMARY_MARKER = "<!-- fabro-code-review-summary -->"
COMMENT_TAG_PREFIX = "fabro-code-review-comment"
RUN_TAG_PREFIX = "fabro-code-review-run"
COMPLETED_TAG_PREFIX = "fabro-code-review-completed"
DEFAULT_BATCH_SIZE = 50
# GitHub caps a comment body at 65,536 characters; the summary is assembled
# under this budget so a write can never fail on size (R19).
SUMMARY_BUDGET = 65000
GITHUB_BODY_CAP = 65536
SEVERITY_RANK = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
SEVERITY_EMOJI = {"LOW": "🟡", "MEDIUM": "🟠", "HIGH": "🔴"}
CANONICAL_FILE_NAMES = (
    "review-manifest.json",
    "candidate-ledger.jsonl",
    "findings.json",
    "coverage.json",
    "votes.jsonl",
)

REVIEW_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
REPO_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}/[A-Za-z0-9][A-Za-z0-9._-]{0,99}$"
)
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
HUNK_HEADER_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
PLAUSIBLE_WARNING = (
    "> **Needs confirmation:** The verifier could not fully confirm this "
    "finding from the available evidence."
)


class PublishError(RuntimeError):
    """A refused plan or apply request."""


def fail(message: str) -> NoReturn:
    raise PublishError(message)


def comment_tag(review_id: str, finding_id: str) -> str:
    return f"{COMMENT_TAG_PREFIX}:{review_id}:{finding_id}"


def run_tag_for(review_id: str) -> str:
    return f"<!-- {RUN_TAG_PREFIX}:{review_id} -->"


def completed_tag_for(completed_at: str) -> str:
    return f"<!-- {COMPLETED_TAG_PREFIX}:{completed_at} -->"


# --- Git arithmetic (plan) ---------------------------------------------------


def run_git(*arguments: str) -> subprocess.CompletedProcess:
    environment = os.environ.copy()
    environment.update(
        {
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_PAGER": "cat",
            "PAGER": "cat",
        }
    )
    try:
        return subprocess.run(
            ["git", "-c", "core.quotePath=false", *arguments],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            check=False,
        )
    except OSError as error:
        fail(f"could not run Git: {error}")


def resolve_commit(token: str, field: str) -> str:
    result = run_git("rev-parse", "--verify", "--quiet", token + "^{commit}")
    resolved = result.stdout.decode("utf-8", "replace").strip()
    if result.returncode != 0 or not SHA_RE.fullmatch(resolved):
        fail(
            f"{field} {token!r} does not resolve to a commit in this "
            "repository; plan must run inside the reviewed checkout"
        )
    return resolved


def resolve_diff_base(token: str) -> str:
    """The diff base as a commit, or a bare tree for a root commit.

    A root commit's reviewed range starts at the empty tree, which is
    tree-ish but not a commit; ``git diff`` accepts it as a base.
    """
    result = run_git("rev-parse", "--verify", "--quiet", token + "^{commit}")
    resolved = result.stdout.decode("utf-8", "replace").strip()
    if result.returncode == 0 and SHA_RE.fullmatch(resolved):
        return resolved
    result = run_git("rev-parse", "--verify", "--quiet", token + "^{tree}")
    resolved = result.stdout.decode("utf-8", "replace").strip()
    if result.returncode == 0 and SHA_RE.fullmatch(resolved):
        return resolved
    fail(
        f"range base {token!r} does not resolve to a commit or tree in "
        "this repository; plan must run inside the reviewed checkout"
    )


def right_side_hunks(base: str, head: str) -> Dict[str, List[Tuple[int, int]]]:
    """RIGHT-side hunk line ranges of ``git diff -U3 base head`` (R2).

    Hunks include context lines; a pure-deletion hunk has no RIGHT-side
    lines and is skipped. A ``+++ `` target line counts as a file header
    only inside a file's preamble (between its ``diff --git`` boundary
    and its first hunk): an added body line whose content starts with
    ``++ `` renders as ``+++ `` but cannot reach the preamble, because
    every hunk body line carries a +/-/space marker while a real file
    boundary starts bare.
    """
    result = run_git(
        "diff", "--no-color", "--no-ext-diff", "--no-textconv",
        "--find-renames", "-U3",
        base, head,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace").strip()
        fail(f"git diff over the reviewed range failed: {detail}")
    ranges: Dict[str, List[Tuple[int, int]]] = {}
    current: Optional[str] = None
    in_preamble = False
    for line in result.stdout.decode("utf-8", "replace").splitlines():
        if line.startswith("diff --git "):
            in_preamble = True
            current = None
        elif in_preamble and line.startswith("+++ "):
            target = line[4:]
            if target == "/dev/null" or target.startswith('"'):
                current = None
            elif target.startswith("b/"):
                current = target[2:]
            else:
                current = target
        elif line.startswith("@@ "):
            in_preamble = False
            if current is None:
                continue
            match = HUNK_HEADER_RE.match(line)
            if not match:
                continue
            start = int(match.group(1))
            count = 1 if match.group(2) is None else int(match.group(2))
            if count > 0:
                ranges.setdefault(current, []).append(
                    (start, start + count - 1)
                )
    return ranges


def has_diff_range(
    hunks: Mapping[str, Sequence[Tuple[int, int]]],
    path: str,
    start_line: int,
    end_line: int,
) -> bool:
    """True when one RIGHT-side hunk contains the complete range."""
    return any(
        hunk_start <= start_line <= end_line <= hunk_end
        for hunk_start, hunk_end in hunks.get(path, ())
    )


# --- Routing configuration (R3-R5, fail-closed) ------------------------------


def parse_severity_threshold(raw: str) -> Optional[str]:
    text = (raw or "").strip().lower()
    if not text:
        return None
    if text.upper() not in SEVERITY_RANK:
        fail(
            f"route-severity-below must be one of high, medium, low "
            f"(or empty to disable); got {raw!r}"
        )
    return text.upper()


def parse_route_categories(raw: str) -> List[str]:
    text = (raw or "").strip()
    if not text:
        return []
    tokens: List[str] = []
    for piece in text.split(","):
        token = piece.strip().lower()
        if not token:
            continue
        if token not in renderer.CATEGORIES:
            fail(
                f"route-categories names an unknown category {token!r}; "
                f"known: {', '.join(renderer.CATEGORIES)}"
            )
        if token not in tokens:
            tokens.append(token)
    return tokens


def parse_batch_size(raw: str) -> int:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return DEFAULT_BATCH_SIZE
    return value if value >= 1 else DEFAULT_BATCH_SIZE


def parse_pr_number(raw: str) -> int:
    text = str(raw).strip()
    if not text.isdigit() or int(text) < 1:
        fail(f"pr must be a positive integer, got {raw!r}")
    return int(text)


def routing_detail(
    finding: Mapping[str, Any],
    threshold: Optional[str],
    categories: Sequence[str],
) -> Optional[str]:
    """The reason a finding routes to the summary, or None (R3, R4)."""
    reasons: List[str] = []
    if threshold is not None and (
        SEVERITY_RANK[finding["severity"]] <= SEVERITY_RANK[threshold]
    ):
        reasons.append(
            f"severity {finding['severity']} is at or below the "
            f"{threshold.lower()} threshold"
        )
    if finding["category"] in categories:
        reasons.append(f"category {finding['category']} is routed by policy")
    return "; ".join(reasons) if reasons else None


# --- Comment and summary rendering (R9, R11) ---------------------------------


def finding_detail_lines(finding: Mapping[str, Any]) -> List[str]:
    lines: List[str] = []
    if finding["summary"].strip() != finding["short_summary"].strip():
        lines.extend(["", renderer.escape_markdown(finding["summary"])])
    lines.extend(
        [
            "",
            "**Failure scenario.** "
            + renderer.escape_markdown(finding["failure_scenario"]),
        ]
    )
    if finding["verdict"] == "UNVERIFIED":
        lines.extend(["", renderer.UNVERIFIED_FINDING_ITALIC])
    else:
        reasoning = str(finding.get("verdict_reasoning") or "").strip()
        if reasoning:
            lines.extend(
                ["", "**Verifier.** " + renderer.escape_markdown(reasoning)]
            )
    return lines


def inline_more_lines(finding: Mapping[str, Any]) -> List[str]:
    verdict = str(finding["verdict"]).lower().capitalize()
    confidence = str(finding["confidence"]).lower().capitalize()
    lines = [
        "",
        "<details>",
        f"<summary>More · {verdict} · {confidence} confidence</summary>",
        "",
        "**Impact:** "
        + renderer.escape_markdown(finding["failure_scenario"]),
        "",
    ]
    reasoning = str(finding.get("verdict_reasoning") or "").strip()
    if reasoning:
        evidence = renderer.escape_markdown(reasoning)
    elif finding["verdict"] == "UNVERIFIED":
        evidence = "No independent verification ran at this effort level."
    else:
        evidence = "No verifier reasoning was recorded."
    lines.append("**Evidence:** " + evidence)

    metadata: List[str] = []
    reports = finding.get("reports")
    reporters = finding.get("reporters") or []
    if isinstance(reports, int) and reports > 1:
        report_text = f"- Reported by {reports} review passes"
        if reporters:
            report_text += ": " + renderer.escape_markdown(
                ", ".join(str(reporter) for reporter in reporters)
            )
        metadata.append(report_text)

    rule_ids = finding.get("rule_ids") or []
    if rule_ids:
        label = "Rule" if len(rule_ids) == 1 else "Rules"
        metadata.append(
            f"- {label}: "
            + ", ".join(renderer.code_span(rule_id) for rule_id in rule_ids)
        )

    for anchor in finding.get("anchors") or []:
        location = f"{anchor['file']}:{anchor['line']}"
        metadata.append(
            f"- Related location: {renderer.code_span(location)} "
            f"({renderer.escape_markdown(anchor['category'])}, "
            f"{renderer.escape_markdown(anchor['id'])})"
        )

    if metadata:
        lines.extend(["", *metadata])
    lines.extend(["", "</details>"])
    return lines


def inline_comment_body(finding: Mapping[str, Any], review_id: str) -> str:
    tag = comment_tag(review_id, finding["id"])
    issue_type = str(finding["issue_type"]).lower().capitalize()
    lines = [
        f"<!-- {tag} -->",
        "",
        f"**{SEVERITY_EMOJI[finding['severity']]} {issue_type}** — "
        + renderer.escape_markdown(finding["short_summary"]),
    ]
    if finding["summary"].strip() != finding["short_summary"].strip():
        lines.extend(["", renderer.escape_markdown(finding["summary"])])
    if finding["verdict"] == "PLAUSIBLE":
        lines.extend(["", PLAUSIBLE_WARNING])
    elif finding["verdict"] == "UNVERIFIED":
        lines.extend(["", renderer.UNVERIFIED_FINDING_WARNING])
    suggestion = finding.get("suggestion")
    if isinstance(suggestion, dict):
        lines.extend(
            [
                "",
                *renderer.fenced_text(
                    suggestion["replacement_code"], "suggestion"
                ),
            ]
        )
    lines.extend(inline_more_lines(finding))
    return "\n".join(lines)


def summary_section(
    finding: Mapping[str, Any], reason_text: Optional[str]
) -> str:
    location_data = finding["location"]
    start_line = location_data["start_line"]
    end_line = location_data["end_line"]
    location_text = (
        f"{finding['file']}:{start_line}"
        if start_line == end_line
        else f"{finding['file']}:{start_line}-{end_line}"
    )
    location = renderer.code_span(location_text)
    facts = [location]
    if reason_text:
        facts.append(reason_text)
    facts.append(f"verdict {finding['verdict']}")
    facts.append(f"confidence {finding['confidence']}")
    rule_ids = finding.get("rule_ids") or []
    if rule_ids:
        facts.append(
            "rule "
            + ", ".join(renderer.code_span(rule_id) for rule_id in rule_ids)
        )
    lines = [
        f"### {finding['id']} · {finding['severity']} "
        f"{finding['issue_type']} / {finding['category']} — "
        + renderer.escape_markdown(finding["short_summary"]),
        "",
        " · ".join(facts),
        *finding_detail_lines(finding),
    ]
    excerpt = renderer.code_block(finding["code"])
    if excerpt:
        lines.extend(["", *excerpt])
    suggestion = finding.get("suggestion")
    if isinstance(suggestion, dict):
        lines.extend(
            [
                "",
                "<details><summary>Suggested change</summary>",
                "",
                "**Before:**",
                *renderer.fenced_text(location_data["existing_code"]),
                "",
                "**After:**",
                *renderer.fenced_text(suggestion["replacement_code"]),
                "",
                "</details>",
            ]
        )
    return "\n".join(lines)


def counts_line(
    total: int, inline: int, no_position: int, routed: int, failed: int
) -> str:
    if total == 0:
        return (
            "**No findings.** The review completed with nothing to report; "
            "this summary supersedes any earlier run."
        )
    return (
        f"**{total} finding(s)** — posted inline: {inline} · "
        f"no diff position: {no_position} · routed by policy: {routed} · "
        f"could not be posted: {failed}"
    )


def rules_coverage_line(
    coverage: Mapping[str, Any], findings: Sequence[Mapping[str, Any]]
) -> Optional[str]:
    rules = coverage.get("rules")
    if not isinstance(rules, dict):
        return None
    effective = rules.get("effectiveChecksByFile")
    effective = effective if isinstance(effective, dict) else {}
    audited_files = sum(1 for check_ids in effective.values() if check_ids)
    distinct = {
        check_id
        for check_ids in effective.values()
        if isinstance(check_ids, list)
        for check_id in check_ids
    }
    counts = rules.get("counts") or {}
    rule_findings = sum(1 for finding in findings if finding.get("rule_ids"))
    return (
        f"Rules: audited {len(distinct)} check(s) "
        f"({counts.get('builtin_packs', 0)} built-in + "
        f"{counts.get('repo_packs', 0)} repository pack(s)) across "
        f"{audited_files} file(s); {rule_findings} rule violation(s) reported."
    )


def summary_context_lines(
    manifest: Mapping[str, Any],
    coverage: Mapping[str, Any],
    findings: Sequence[Mapping[str, Any]],
    reasons: Sequence[str],
    head: str,
    run_url: str,
) -> List[str]:
    lines: List[str] = []
    if reasons:
        lines.append(renderer.partial_review_warning(reasons))
    if manifest["effort"] == "low":
        lines.append(renderer.LOW_EFFORT_REVIEW_NOTE)
    rules_text = rules_coverage_line(coverage, findings)
    if rules_text:
        lines.append(rules_text)
    lines.append(
        f"Review {renderer.code_span(manifest['review_id'])} · "
        f"effort {manifest['effort']} · mode {manifest['mode']} · "
        f"commit {renderer.code_span(head[:12])} · completed "
        + renderer.escape_markdown(manifest.get("completed_at"))
    )
    if run_url:
        lines.append(f"Run report: {run_url}")
    return lines


def elision_line(count: int, review_id: str, run_url: str) -> str:
    reference = f"see review {review_id}"
    if run_url:
        reference += f" and the run report: {run_url}"
    return f"_{count} finding(s) omitted from this summary; {reference}._"


def assemble_summary_body(
    marker: str,
    run_tag: str,
    completed_tag: str,
    counts_text: str,
    context_lines: Sequence[str],
    sections: Sequence[str],
    review_id: str,
    run_url: str,
) -> str:
    """Assemble the sticky summary under the size budget (R9, R19).

    Sections render in full in ranking order; when the next section would
    overflow the budget, it and every later section are replaced by one
    elision line. Elision affects rendering only, never counts.
    """
    head_parts = [marker, run_tag, completed_tag, "", "## Code review", "",
                  counts_text]
    for line in context_lines:
        head_parts.extend(["", line])
    if sections:
        head_parts.extend(["", "### Findings not posted inline"])
    head = "\n".join(head_parts)
    for chosen in range(len(sections), 0, -1):
        omitted = len(sections) - chosen
        candidate = head + "".join(
            "\n\n" + section for section in sections[:chosen]
        )
        if omitted:
            candidate += "\n\n" + elision_line(omitted, review_id, run_url)
        if len(candidate) <= SUMMARY_BUDGET:
            return candidate
    body = head
    if sections:
        body += "\n\n" + elision_line(len(sections), review_id, run_url)
    return body


# --- plan --------------------------------------------------------------------


def bundle_digest(evidence_dir: str) -> str:
    hasher = hashlib.sha256()
    for name in CANONICAL_FILE_NAMES:
        path = Path(evidence_dir) / name
        try:
            raw = path.read_bytes()
        except OSError as error:
            fail(f"could not read canonical file {path}: {error}")
        hasher.update(name.encode("utf-8"))
        hasher.update(b"\x00")
        hasher.update(hashlib.sha256(raw).digest())
    return hasher.hexdigest()


def load_bundle(
    evidence_dir: str,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any], List[str]]:
    manifest = renderer.validate_manifest(
        renderer.read_json(evidence_dir, "review-manifest.json")
    )
    findings = renderer.validate_findings(
        renderer.read_json(evidence_dir, "findings.json")
    )
    ledger = renderer.validate_ledger(
        renderer.read_jsonl(evidence_dir, "candidate-ledger.jsonl")
    )
    votes = renderer.validate_votes(
        renderer.read_jsonl(evidence_dir, "votes.jsonl")
    )
    coverage = renderer.validate_coverage(
        renderer.read_json(evidence_dir, "coverage.json")
    )
    renderer.validate_relationships(
        manifest, findings, ledger, votes, coverage
    )
    reasons = renderer.partial_reasons(manifest, coverage)
    return manifest, findings, coverage, reasons


def resolve_reviewed_range(manifest: Mapping[str, Any]) -> Tuple[str, str, str]:
    """The reviewed (base, head, range) as local commits (R2, R20)."""
    if manifest["mode"] not in ("changes", "commit"):
        fail(
            "the publisher requires a ranged review (mode changes or "
            "commit); a files-mode bundle has no PR diff to anchor to"
        )
    range_text = manifest.get("range")
    if not isinstance(range_text, str) or not range_text.strip():
        fail("the manifest has no reviewed range")
    range_text = range_text.strip()
    revision = manifest.get("revision")
    if not isinstance(revision, dict) or not revision.get("versioned"):
        fail("the manifest has no versioned revision record")
    head = revision.get("commit")
    if not isinstance(head, str) or not SHA_RE.fullmatch(head):
        fail("the manifest revision does not name the reviewed head commit")
    if "..." in range_text:
        left_token = range_text.split("...", 1)[0]
        three_dot = True
    elif ".." in range_text:
        left_token = range_text.split("..", 1)[0]
        three_dot = False
    else:
        fail(f"the manifest range is not two-sided: {range_text!r}")
    if not left_token:
        fail(f"the manifest range has no base side: {range_text!r}")
    left_sha = resolve_diff_base(left_token)
    resolved_head = resolve_commit(head, "reviewed head")
    if resolved_head != head:
        fail("the reviewed head commit is not present in this repository")
    if three_dot:
        result = run_git("merge-base", left_sha, head)
        base = result.stdout.decode("utf-8", "replace").strip()
        if result.returncode != 0 or not SHA_RE.fullmatch(base):
            fail("the reviewed range endpoints have no merge base")
    else:
        base = left_sha
    return base, head, range_text


def command_plan(args: argparse.Namespace) -> int:
    repo = args.repo.strip()
    if not REPO_RE.fullmatch(repo) or ".." in repo:
        fail(f"repo must look like owner/name, got {args.repo!r}")
    pr = parse_pr_number(args.pr)
    # Fail-closed routing policy (R5): a malformed configuration fails the
    # plan before anything can be posted.
    threshold = parse_severity_threshold(args.route_severity_below)
    categories = parse_route_categories(args.route_categories)
    batch_size = parse_batch_size(args.batch_size)
    run_url = (args.run_url or "").strip()
    if len(run_url) > 2048:
        fail("run-url exceeds 2048 characters")

    manifest, findings, coverage, reasons = load_bundle(args.evidence_dir)
    base, head, range_text = resolve_reviewed_range(manifest)
    review_id = str(manifest["review_id"])
    completed_at = manifest.get("completed_at")
    if not isinstance(completed_at, str) or not completed_at.strip():
        fail("the manifest has no completion timestamp")

    hunks = right_side_hunks(base, head)

    # Exhaustive partition (R1): every finding gets exactly one placement.
    # Diff position decides first (R2); routing applies only to otherwise
    # inline-eligible findings, so a finding matching both carries the
    # no-position reason and routing can never hide a placement.
    placements: List[Dict[str, Any]] = []
    for finding in findings:
        location = finding["location"]
        start_line = location["start_line"]
        end_line = location["end_line"]
        base_entry = {
            "finding_id": finding["id"],
            "path": finding["file"],
            "line": end_line,
            "start_line": start_line,
            "end_line": end_line,
        }
        if not has_diff_range(
            hunks, finding["file"], start_line, end_line
        ):
            placements.append(
                {
                    **base_entry,
                    "placement": "summary",
                    "reason": "no-position",
                    "body": summary_section(
                        finding, "no diff position in the reviewed range"
                    ),
                }
            )
            continue
        detail = routing_detail(finding, threshold, categories)
        if detail is not None:
            placements.append(
                {
                    **base_entry,
                    "placement": "summary",
                    "reason": "routed",
                    "detail": detail,
                    "body": summary_section(
                        finding, f"routed to the summary by policy — {detail}"
                    ),
                }
            )
            continue
        placements.append(
            {
                **base_entry,
                "placement": "inline",
                "comment_id": comment_tag(review_id, finding["id"]),
                "body": inline_comment_body(finding, review_id),
                "section": summary_section(finding, None),
            }
        )

    inline_entries = [
        entry for entry in placements if entry["placement"] == "inline"
    ]
    no_position = sum(
        1
        for entry in placements
        if entry["placement"] == "summary" and entry["reason"] == "no-position"
    )
    routed = sum(
        1
        for entry in placements
        if entry["placement"] == "summary" and entry["reason"] == "routed"
    )

    # Deterministic batching (R13): sorted (path, line, finding ID), then
    # contiguous chunks of at most batch_size. Routed findings never enter
    # a batch (R6).
    ordered = sorted(
        inline_entries,
        key=lambda entry: (
            entry["path"],
            entry["line"],
            int(entry["finding_id"][1:]),
        ),
    )
    batches = [
        [entry["finding_id"] for entry in ordered[index:index + batch_size]]
        for index in range(0, len(ordered), batch_size)
    ]

    run_tag = run_tag_for(review_id)
    completed_tag = completed_tag_for(completed_at)
    context = summary_context_lines(
        manifest, coverage, findings, reasons, head, run_url
    )
    sections = [
        entry["body"] for entry in placements if entry["placement"] == "summary"
    ]
    summary_body = assemble_summary_body(
        SUMMARY_MARKER,
        run_tag,
        completed_tag,
        counts_line(len(findings), len(inline_entries), no_position, routed, 0),
        context,
        sections,
        review_id,
        run_url,
    )
    anchor_body = "\n".join(
        [
            SUMMARY_MARKER,
            run_tag,
            completed_tag,
            "",
            "## Code review",
            "",
            "_Posting code review results…_",
        ]
    )

    plan = {
        "version": PLAN_VERSION,
        "review_id": review_id,
        "mode": manifest["mode"],
        "effort": manifest["effort"],
        "target": {"repo": repo, "pr": pr},
        "base": base,
        "head": head,
        "range": range_text,
        "bundle_digest": bundle_digest(args.evidence_dir),
        "config": {
            "batch_size": batch_size,
            "route_severity_below": threshold.lower() if threshold else "",
            "route_categories": categories,
            "run_url": run_url,
        },
        "placements": placements,
        "batches": batches,
        "summary": {
            "marker": SUMMARY_MARKER,
            "run_tag": run_tag,
            "completed_tag": completed_tag,
            "completed_at": completed_at,
            "anchor_body": anchor_body,
            "body": summary_body,
            "context_lines": context,
        },
        "counts": {
            "total": len(findings),
            "planned_inline": len(inline_entries),
            "no_position": no_position,
            "routed": routed,
            "skipped": 0,
        },
    }
    output = Path(args.output)
    temporary = output.with_name(output.name + ".tmp")
    temporary.write_text(
        json.dumps(plan, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, output)
    print(
        f"Planned {len(findings)} placement(s): {len(inline_entries)} inline "
        f"in {len(batches)} batch(es), {no_position} without a diff "
        f"position, {routed} routed"
    )
    return 0


# --- Plan re-validation (apply-side, R18) ------------------------------------


def require_text(value: Any, field: str, limit: int) -> str:
    if not isinstance(value, str) or not value or len(value) > limit:
        fail(f"plan {field} must be a string of at most {limit} characters")
    return value


def validate_plan_document(
    value: Any, repo: str, pr: int
) -> Dict[str, Any]:
    """Re-validate the untrusted plan before any write (R18)."""
    if not isinstance(value, dict):
        fail("the plan is not a JSON object")
    if value.get("version") != PLAN_VERSION:
        fail("the plan has an unsupported version")
    review_id = value.get("review_id")
    if not isinstance(review_id, str) or not REVIEW_ID_RE.fullmatch(review_id):
        fail("the plan review_id is invalid")
    target = value.get("target")
    if not isinstance(target, dict) or target.get("repo") != repo or (
        target.get("pr") != pr
    ):
        fail(
            "the plan's embedded target does not match --repo/--pr; refusing"
        )
    for field in ("base", "head"):
        sha = value.get(field)
        if not isinstance(sha, str) or not SHA_RE.fullmatch(sha):
            fail(f"the plan {field} is not a commit SHA")
    config = value.get("config")
    if not isinstance(config, dict):
        fail("the plan config is missing")
    run_url = config.get("run_url", "")
    if not isinstance(run_url, str) or len(run_url) > 2048:
        fail("the plan run_url is invalid")

    placements = value.get("placements")
    if not isinstance(placements, list):
        fail("the plan placements must be an array")
    seen_ids: Set[str] = set()
    inline_ids: List[str] = []
    reason_counts = {"no-position": 0, "routed": 0}
    for index, entry in enumerate(placements):
        field = f"placements[{index}]"
        if not isinstance(entry, dict):
            fail(f"plan {field} must be an object")
        finding_id = entry.get("finding_id")
        if not isinstance(finding_id, str) or not FINDING_ID_RE.fullmatch(
            finding_id
        ):
            fail(f"plan {field}.finding_id is invalid")
        if finding_id in seen_ids:
            fail(f"plan {field} repeats finding {finding_id}")
        seen_ids.add(finding_id)
        try:
            renderer.safe_repo_path(entry.get("path"), f"plan {field}.path")
        except renderer.RenderError as error:
            fail(str(error))
        line = entry.get("line")
        if isinstance(line, bool) or not isinstance(line, int) or line < 1:
            fail(f"plan {field}.line must be a positive integer")
        start_line = entry.get("start_line")
        end_line = entry.get("end_line")
        if (
            isinstance(start_line, bool)
            or not isinstance(start_line, int)
            or start_line < 1
        ):
            fail(f"plan {field}.start_line must be a positive integer")
        if (
            isinstance(end_line, bool)
            or not isinstance(end_line, int)
            or end_line < start_line
            or end_line != line
        ):
            fail(
                f"plan {field}.end_line must end at its line and not precede "
                "start_line"
            )
        body = require_text(entry.get("body"), f"{field}.body", GITHUB_BODY_CAP)
        placement = entry.get("placement")
        if placement == "inline":
            expected_tag = comment_tag(review_id, finding_id)
            if entry.get("comment_id") != expected_tag:
                fail(f"plan {field}.comment_id is not this review's tag")
            if f"<!-- {expected_tag} -->" not in body:
                fail(f"plan {field}.body does not embed its identity tag")
            if "section" in entry:
                require_text(
                    entry.get("section"), f"{field}.section", GITHUB_BODY_CAP
                )
            inline_ids.append(finding_id)
        elif placement == "summary":
            reason = entry.get("reason")
            if reason not in reason_counts:
                fail(f"plan {field}.reason is invalid")
            reason_counts[reason] += 1
            if "detail" in entry:
                require_text(entry.get("detail"), f"{field}.detail", 2000)
        else:
            fail(f"plan {field}.placement is invalid")

    batches = value.get("batches")
    if not isinstance(batches, list) or not all(
        isinstance(batch, list) and batch for batch in batches
    ):
        fail("the plan batches must be an array of non-empty arrays")
    batched = [finding_id for batch in batches for finding_id in batch]
    if len(batched) != len(set(batched)) or set(batched) != set(inline_ids):
        fail(
            "the plan batches do not partition exactly the inline "
            "placements (R6)"
        )

    summary = value.get("summary")
    if not isinstance(summary, dict):
        fail("the plan summary is missing")
    if summary.get("marker") != SUMMARY_MARKER:
        fail("the plan summary marker is not this workflow's marker")
    if summary.get("run_tag") != run_tag_for(review_id):
        fail("the plan summary run_tag is not this review's tag")
    completed_at = require_text(
        summary.get("completed_at"), "summary.completed_at", 64
    )
    if summary.get("completed_tag") != completed_tag_for(completed_at):
        fail("the plan summary completed_tag is inconsistent")
    for field in ("anchor_body", "body"):
        body = require_text(
            summary.get(field), f"summary.{field}", GITHUB_BODY_CAP
        )
        if SUMMARY_MARKER not in body:
            fail(f"the plan summary.{field} does not embed the marker")
    context = summary.get("context_lines")
    if not isinstance(context, list) or len(context) > 100 or not all(
        isinstance(line, str) and len(line) <= GITHUB_BODY_CAP
        for line in context
    ):
        fail("the plan summary.context_lines are invalid")

    counts = value.get("counts")
    if not isinstance(counts, dict):
        fail("the plan counts are missing")
    for field in ("total", "planned_inline", "no_position", "routed", "skipped"):
        entry = counts.get(field)
        if isinstance(entry, bool) or not isinstance(entry, int) or entry < 0:
            fail(f"plan counts.{field} must be a non-negative integer")
    if (
        counts["total"] != len(placements)
        or counts["planned_inline"] != len(inline_ids)
        or counts["no_position"] != reason_counts["no-position"]
        or counts["routed"] != reason_counts["routed"]
        or counts["skipped"] != 0
    ):
        fail("the plan counts do not reconcile with its placements (R1)")
    return value


# --- GitHub client (apply) ---------------------------------------------------


class GitHubClient:
    def __init__(self, api_base: str, token: str) -> None:
        self.api_base = api_base.rstrip("/")
        self.token = token

    def request(
        self, method: str, path: str, payload: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[int], Any]:
        """(status, parsed JSON); status None means a network failure."""
        data = (
            json.dumps(payload).encode("utf-8") if payload is not None else None
        )
        request = urllib.request.Request(
            self.api_base + path,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "fabro-code-review-publisher",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
                status = response.status
        except urllib.error.HTTPError as error:
            raw = error.read()
            status = error.code
        except (urllib.error.URLError, OSError):
            return None, None
        try:
            return status, json.loads(raw) if raw else None
        except json.JSONDecodeError:
            return status, None

    def list_all(self, path: str) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for page in range(1, 51):
            status, value = self.request(
                "GET", f"{path}?per_page=100&page={page}"
            )
            if status != 200 or not isinstance(value, list):
                fail(f"could not list {path} (HTTP {status})")
            results.extend(
                entry for entry in value if isinstance(entry, dict)
            )
            if len(value) < 100:
                break
        return results


def extract_posted_ids(
    comments: Sequence[Mapping[str, Any]], review_id: str
) -> Set[str]:
    pattern = re.compile(
        r"<!-- "
        + re.escape(f"{COMMENT_TAG_PREFIX}:{review_id}:")
        + r"(R[0-9]+) -->"
    )
    posted: Set[str] = set()
    for comment in comments:
        for match in pattern.finditer(str(comment.get("body") or "")):
            posted.add(match.group(1))
    return posted


def completed_stamp(body: str) -> Optional[str]:
    match = re.search(
        r"<!-- " + re.escape(COMPLETED_TAG_PREFIX) + r":([^>]+) -->", body
    )
    return match.group(1).strip() if match else None


def is_newer_stamp(existing: Optional[str], ours: str) -> bool:
    if existing is None:
        return False
    try:
        return datetime.fromisoformat(existing) > datetime.fromisoformat(ours)
    except (ValueError, TypeError):
        return existing > ours


def strip_identity_tag(body: str) -> str:
    return re.sub(
        r"^<!-- " + re.escape(COMMENT_TAG_PREFIX) + r":[^>]+ -->\n*",
        "",
        body,
    )


# --- apply -------------------------------------------------------------------


class BatchPoster:
    """Posts inline batches with never-duplicate discipline (R13-R15)."""

    def __init__(
        self,
        client: GitHubClient,
        repo: str,
        pr: int,
        plan: Mapping[str, Any],
        already_posted: Set[str],
    ) -> None:
        self.client = client
        self.repo = repo
        self.pr = pr
        self.head = plan["head"]
        self.by_id = {
            entry["finding_id"]: entry
            for entry in plan["placements"]
            if entry["placement"] == "inline"
        }
        self.review_id = plan["review_id"]
        self.posted: Set[str] = set(already_posted)
        self.failed: Dict[str, str] = {}
        self.batches_attempted = 0
        self.batches_succeeded = 0

    def post_review(self, finding_ids: Sequence[str]) -> Optional[int]:
        comments: List[Dict[str, Any]] = []
        for finding_id in finding_ids:
            entry = self.by_id[finding_id]
            comment: Dict[str, Any] = {
                "path": entry["path"],
                "line": entry["end_line"],
                "side": "RIGHT",
                "body": entry["body"],
            }
            if entry["start_line"] != entry["end_line"]:
                comment["start_line"] = entry["start_line"]
                comment["start_side"] = "RIGHT"
            comments.append(comment)
        # No review body: the run tag lives in the sticky summary and in
        # every inline comment's identity tag, so body text here would
        # only add a noise bubble to the PR timeline (R12).
        status, _ = self.client.request(
            "POST",
            f"/repos/{self.repo}/pulls/{self.pr}/reviews",
            {
                "commit_id": self.head,
                "event": "COMMENT",
                "comments": comments,
            },
        )
        return status

    def landed_ids(self) -> Optional[Set[str]]:
        """The finding IDs whose comments are on the PR, or None if the
        read failed (then nothing can be verified, R14)."""
        try:
            comments = self.client.list_all(
                f"/repos/{self.repo}/pulls/{self.pr}/comments"
            )
        except PublishError:
            return None
        return extract_posted_ids(comments, self.review_id)

    @staticmethod
    def is_server_failure(status: Optional[int]) -> bool:
        return status is None or status == 408 or (status >= 500)

    UNVERIFIED_DETAIL = (
        "a server error interrupted the write and the result could not "
        "be verified"
    )
    DROPPED_DETAIL = (
        "a server error dropped the write; the comment was verified "
        "missing and the retry also failed"
    )

    def mark_failed(self, finding_ids: Sequence[str], detail: str) -> None:
        for finding_id in finding_ids:
            self.failed[finding_id] = detail

    def reconcile(self, finding_ids: Sequence[str]) -> List[str]:
        """After a possibly-landed failure: absorb what landed, return what
        is verifiably missing; on an unverifiable read, mark failed and
        return nothing (never risk a duplicate, R14)."""
        landed = self.landed_ids()
        if landed is None:
            self.mark_failed(finding_ids, self.UNVERIFIED_DETAIL)
            return []
        self.posted.update(landed & set(self.by_id))
        return [fid for fid in finding_ids if fid not in landed]

    def post_individual_pass(self, finding_ids: Sequence[str]) -> List[str]:
        """Post once and return writes with an ambiguous server result."""
        ambiguous: List[str] = []
        for finding_id in finding_ids:
            status = self.post_review([finding_id])
            if status in (200, 201):
                self.posted.add(finding_id)
            elif status == 422:
                self.failed[finding_id] = (
                    "GitHub could not resolve the diff position (422)"
                )
            elif self.is_server_failure(status):
                ambiguous.append(finding_id)
            else:
                self.failed[finding_id] = f"GitHub refused the comment (HTTP {status})"
        return ambiguous

    def post_individually(self, finding_ids: Sequence[str]) -> None:
        """Per-comment fallback that isolates unpostable comments (R15).

        Reconcile ambiguous writes once per pass. This preserves duplicate
        safety without re-paginating the complete comment history for every
        failed comment.
        """
        ambiguous = self.post_individual_pass(finding_ids)
        if not ambiguous:
            return
        missing = self.reconcile(ambiguous)
        if not missing:
            return
        # Verified missing, so one retry cannot duplicate (R14).
        retry_ambiguous = self.post_individual_pass(missing)
        if retry_ambiguous:
            still_missing = self.reconcile(retry_ambiguous)
            if still_missing:
                self.mark_failed(still_missing, self.DROPPED_DETAIL)

    def post_batch(self, batch_ids: Sequence[str]) -> None:
        to_send = [fid for fid in batch_ids if fid not in self.posted]
        if to_send:
            self.batches_attempted += 1
            status = self.post_review(to_send)
            if status in (200, 201):
                self.posted.update(to_send)
            elif status == 422:
                self.post_individually(to_send)
            elif self.is_server_failure(status):
                missing = self.reconcile(to_send)
                if missing:
                    retry_status = self.post_review(missing)
                    if retry_status in (200, 201):
                        self.posted.update(missing)
                    elif retry_status == 422:
                        self.post_individually(missing)
                    else:
                        still_missing = self.reconcile(missing)
                        if still_missing:
                            self.mark_failed(
                                still_missing, self.DROPPED_DETAIL
                            )
            else:
                for finding_id in to_send:
                    self.failed[finding_id] = (
                        f"GitHub refused the batch (HTTP {status})"
                    )
        if all(fid in self.posted for fid in batch_ids):
            self.batches_succeeded += 1


def command_apply(args: argparse.Namespace) -> int:
    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        fail("apply requires GITHUB_TOKEN in the environment, never argv")
    repo = args.repo.strip()
    if not REPO_RE.fullmatch(repo) or ".." in repo:
        fail(f"repo must look like owner/name, got {args.repo!r}")
    pr = parse_pr_number(args.pr)
    try:
        raw_plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        fail(f"could not read the plan: {error}")
    plan = validate_plan_document(raw_plan, repo, pr)
    review_id = plan["review_id"]
    summary = plan["summary"]
    client = GitHubClient(args.api_base, token)

    # Head drift check before the first write (R20).
    status, pull = client.request("GET", f"/repos/{repo}/pulls/{pr}")
    if status != 200 or not isinstance(pull, dict):
        fail(f"could not read the PR (HTTP {status})")
    live_head = (pull.get("head") or {}).get("sha")
    if live_head != plan["head"]:
        fail(
            f"the live PR head {str(live_head)[:12]} does not match the "
            f"plan's reviewed head {plan['head'][:12]}; refusing to post "
            "against a drifted head"
        )

    # Token identity (R7). A PAT answers /user; a GitHub App installation
    # token gets 403 there, so its login is learned from apply's own first
    # write (the anchor response) below.
    status, user = client.request("GET", "/user")
    login: Optional[str] = None
    if status == 200 and isinstance(user, dict) and user.get("login"):
        login = str(user["login"])
    elif status == 401:
        fail("GitHub rejected the token (HTTP 401)")

    # Reconcile against comments already carrying this review's tags (R14).
    already_posted = extract_posted_ids(
        client.list_all(f"/repos/{repo}/pulls/{pr}/comments"), review_id
    )

    # Sticky-summary discovery (R7): only a marker comment authored by our
    # own token identity is ever updated; the newest owned one wins. While
    # the login is still unknown, no comment counts as owned.
    def find_owned_summary(
        comments: Sequence[Mapping[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if login is None:
            return None
        owned = [
            comment
            for comment in comments
            if SUMMARY_MARKER in str(comment.get("body") or "")
            and (comment.get("user") or {}).get("login") == login
            and isinstance(comment.get("id"), int)
        ]
        if not owned:
            return None
        return max(owned, key=lambda comment: comment["id"])

    prior_comments = client.list_all(f"/repos/{repo}/issues/{pr}/comments")
    summary_comment_id: Optional[int] = None
    summary_url = ""
    stale_skip = False
    anchor_failed = False

    # Take over an owned summary comment as the one to update -- unless it
    # carries a newer review's completed tag: the stale-run guard (R14)
    # never overwrites a newer review's summary.
    def adopt(existing: Mapping[str, Any]) -> None:
        nonlocal stale_skip, summary_comment_id, summary_url
        summary_url = str(existing.get("html_url") or "")
        stamp = completed_stamp(str(existing.get("body") or ""))
        if is_newer_stamp(stamp, summary["completed_at"]):
            stale_skip = True
            summary_comment_id = None
        else:
            summary_comment_id = existing["id"]

    existing = find_owned_summary(prior_comments)
    if existing is not None:
        adopt(existing)

    # Anchor before review on a cold start (R8). A failed anchor does not
    # abort the run; the final summary write settles the outcome (R16).
    # With an unknown login the anchor doubles as the identity probe: its
    # response names our author, and if an older owned sticky comment then
    # turns out to exist, the probe is deleted so that comment stays the
    # one summary (R7); if the delete fails, the probe is the newest owned
    # comment and later runs converge on it.
    if not stale_skip and summary_comment_id is None:
        status, created = client.request(
            "POST",
            f"/repos/{repo}/issues/{pr}/comments",
            {"body": summary["anchor_body"]},
        )
        if status in (200, 201) and isinstance(created, dict) and isinstance(
            created.get("id"), int
        ):
            summary_comment_id = created["id"]
            summary_url = str(created.get("html_url") or "")
            if login is None:
                login = (created.get("user") or {}).get("login")
                prior = find_owned_summary(prior_comments)
                if prior is not None:
                    delete_status, _ = client.request(
                        "DELETE",
                        f"/repos/{repo}/issues/comments/{summary_comment_id}",
                    )
                    deleted = delete_status in (200, 204)
                    if deleted or is_newer_stamp(
                        completed_stamp(str(prior.get("body") or "")),
                        summary["completed_at"],
                    ):
                        adopt(prior)
        else:
            anchor_failed = True
            print(
                f"warning: could not create the summary anchor (HTTP {status})",
                file=sys.stderr,
            )

    poster = BatchPoster(client, repo, pr, plan, already_posted)
    for batch in plan["batches"]:
        poster.post_batch(batch)

    inline_entries = [
        entry for entry in plan["placements"] if entry["placement"] == "inline"
    ]
    posted_inline = sum(
        1 for entry in inline_entries if entry["finding_id"] in poster.posted
    )
    outcome_counts = {
        "total": plan["counts"]["total"],
        "posted_inline": posted_inline,
        "failed_inline": len(poster.failed),
        "no_position": plan["counts"]["no_position"],
        "routed": plan["counts"]["routed"],
        "skipped": plan["counts"]["skipped"],
    }

    # Final summary: planned sections plus every failed finding with its
    # reason (R15), re-assembled under the same budget (R19).
    summary_failed = False
    if not stale_skip:
        sections = [
            entry["body"]
            for entry in plan["placements"]
            if entry["placement"] == "summary"
        ]
        for entry in inline_entries:
            detail = poster.failed.get(entry["finding_id"])
            if detail is None:
                continue
            section_text = entry.get("section") or strip_identity_tag(
                entry["body"]
            )
            sections.append(
                f"_This finding could not be posted inline: {detail}._"
                + "\n\n"
                + section_text
            )
        final_body = assemble_summary_body(
            SUMMARY_MARKER,
            summary["run_tag"],
            summary["completed_tag"],
            counts_line(
                outcome_counts["total"],
                outcome_counts["posted_inline"],
                outcome_counts["no_position"],
                outcome_counts["routed"],
                outcome_counts["failed_inline"],
            ),
            summary["context_lines"],
            sections,
            review_id,
            plan["config"].get("run_url") or "",
        )
        if summary_comment_id is None and anchor_failed and login is not None:
            # The anchor write may have landed despite its error; re-read
            # before choosing create over update (R14).
            try:
                landed = find_owned_summary(
                    client.list_all(f"/repos/{repo}/issues/{pr}/comments")
                )
            except PublishError:
                landed = None
            if landed is not None:
                summary_comment_id = landed["id"]
                summary_url = str(landed.get("html_url") or "")
        if summary_comment_id is not None:
            status, updated = client.request(
                "PATCH",
                f"/repos/{repo}/issues/comments/{summary_comment_id}",
                {"body": final_body},
            )
        else:
            status, updated = client.request(
                "POST",
                f"/repos/{repo}/issues/{pr}/comments",
                {"body": final_body},
            )
        if status in (200, 201) and isinstance(updated, dict):
            summary_url = str(updated.get("html_url") or summary_url)
        else:
            summary_failed = True
            summary_url = ""
            print(
                f"error: the summary could not be written (HTTP {status}); "
                "posted inline comments are kept",
                file=sys.stderr,
            )

    outcome: Dict[str, Any] = {
        "review_id": review_id,
        "counts": outcome_counts,
        "summary_url": summary_url,
        "batches": {
            "total": poster.batches_attempted,
            "succeeded": poster.batches_succeeded,
        },
        "failures": [
            {"finding_id": finding_id, "detail": detail}
            for finding_id, detail in sorted(poster.failed.items())
        ],
    }
    if stale_skip:
        outcome["summary_skipped"] = (
            "a newer review's summary is already posted"
        )
    if summary_failed:
        outcome["summary_error"] = "the summary comment could not be written"
    outcome_path = Path(args.outcome)
    temporary = outcome_path.with_name(outcome_path.name + ".tmp")
    temporary.write_text(
        json.dumps(outcome, ensure_ascii=True, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, outcome_path)
    print(
        f"Applied: {posted_inline} inline comment(s) posted, "
        f"{len(poster.failed)} failed; summary "
        + (
            "skipped (newer review present)"
            if stale_skip
            else ("FAILED" if summary_failed else "written")
        )
    )
    return 1 if summary_failed else 0


# --- Entry point -------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="publish_pr.py",
        description="Deterministic PR publisher (plan, then apply)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan", help="compute a publication plan")
    plan.add_argument("--evidence-dir", required=True)
    plan.add_argument("--repo", required=True)
    plan.add_argument("--pr", required=True, type=int)
    plan.add_argument("--route-severity-below", default="")
    plan.add_argument("--route-categories", default="")
    plan.add_argument("--batch-size", default=str(DEFAULT_BATCH_SIZE))
    plan.add_argument("--run-url", default="")
    plan.add_argument("--output", required=True)
    plan.set_defaults(handler=command_plan)

    apply_ = commands.add_parser("apply", help="execute a publication plan")
    apply_.add_argument("--plan", required=True)
    apply_.add_argument("--repo", required=True)
    apply_.add_argument("--pr", required=True, type=int)
    apply_.add_argument("--api-base", required=True)
    apply_.add_argument("--outcome", required=True)
    apply_.set_defaults(handler=command_apply)
    return parser


def main(argv: Sequence[str]) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except renderer.RenderError as error:
        print(f"publish_pr.py: invalid bundle: {error}", file=sys.stderr)
        return 2
    except PublishError as error:
        print(f"publish_pr.py: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
