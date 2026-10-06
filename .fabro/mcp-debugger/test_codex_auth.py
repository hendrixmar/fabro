#!/usr/bin/env python3
"""Offline wrapper regression: shared rotating auth, private container homes."""

import base64
import fcntl
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tempfile
import time


SCRIPT = Path(__file__).resolve()
WRAPPER = SCRIPT.with_name("codex-acp-wrapper")
INITIAL_NONCE = "synthetic-refresh-0"


def save_json(path, value):
    # Native Codex truncates the existing inode; replacing it breaks file binds.
    with path.open("w") as stream:
        json.dump(value, stream)


def authenticator():
    """Independent one-use issuer; it never reads the wrapper's credential file."""
    nonce = sys.stdin.read()
    ledger = Path(os.environ["FAKE_AUTH_LEDGER"])
    with ledger.with_suffix(".lock").open("r+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = json.loads(ledger.read_text())
        if nonce in state["redeemed"]:
            print("already redeemed nonce", file=sys.stderr)
            return 23
        if nonce != state["current"]:
            print("unknown nonce", file=sys.stderr)
            return 24
        state["redeemed"].append(nonce)
        state["current"] = f"synthetic-refresh-{len(state['redeemed'])}"
        save_json(ledger, state)
        print(json.dumps({"refresh_token": state["current"]}))
    return 0


def await_file(path, process=None):
    deadline = time.monotonic() + 5
    while not path.exists():
        if process is not None and process.poll() is not None:
            raise AssertionError("wrapper exited before the requested lifecycle event")
        if time.monotonic() >= deadline:
            raise AssertionError("timed out waiting for lifecycle event")
        time.sleep(0.01)


def fake_codex():
    home = Path(os.environ["HOME"])
    started = home / "child-started"
    started.touch()
    if os.environ.get("FAKE_REQUIRE_CONFIG"):
        config = json.loads(os.environ["CODEX_CONFIG"])
        assert config["cli_auth_credentials_store"] == "file"
        assert config["forced_login_method"] == "chatgpt"
        assert config["model"] == "synthetic-model"
        assert config["model_reasoning_effort"] == "high"
        assert config["nested"] == {"keep": [1, 2]}
    codex_home = home / ".codex"
    assert not (codex_home / "sessions").exists(), "private conversations leaked"
    (codex_home / "sessions").mkdir()
    (codex_home / "sessions" / "conversation").write_text(home.name)
    (codex_home / "config.toml").write_text(f'private_session = "{home.name}"\n')
    (codex_home / "cache").mkdir()
    (codex_home / "cache" / "private").write_text(home.name)
    auth_path = codex_home / "auth.json"
    auth = json.loads(auth_path.read_text())
    (home / "credential-read").touch()
    if os.environ.get("FAKE_PAUSE_BEFORE_REDEEM"):
        await_file(home / "release")
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--authenticator"],
        input=auth["tokens"]["refresh_token"], text=True, capture_output=True,
    )
    if result.returncode:
        sys.stderr.write(result.stderr)
        return result.returncode
    auth["tokens"].update(json.loads(result.stdout))
    save_json(auth_path, auth)
    if os.environ.get("FAKE_CANCEL_AFTER_ROTATION"):
        # Worst case: a native CLI descendant inherits the wrapper's lock fd.
        descendant = subprocess.Popen(
            [sys.executable, str(SCRIPT), "--descendant"],
            close_fds=False, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        await_file(home / "descendant-ready", descendant)
        (home / "rotated").touch()
        while True:
            signal.pause()
    (home / "rotated").touch()
    return 17 if os.environ.get("FAKE_FAIL_AFTER_ROTATION") else 0


def regression():
    processes = []
    homes = []
    with tempfile.TemporaryDirectory(prefix="fabro-codex-auth-") as directory:
        root = Path(directory)
        profile = root / "profile"
        profile.mkdir(mode=0o700)
        auth_path = profile / "auth.json"
        initial_auth = {"auth_mode": "chatgpt", "tokens": {
            "access_token": "synthetic-access", "refresh_token": INITIAL_NONCE,
        }}
        save_json(auth_path, initial_auth)
        (profile / "auth.lock").touch(mode=0o600)
        auth_path.chmod(0o600)
        inode = auth_path.stat().st_ino
        ledger = root / "issuer.json"
        save_json(ledger, {"current": INITIAL_NONCE, "redeemed": []})
        ledger.with_suffix(".lock").touch(mode=0o600)
        executables = root / "bin"
        executables.mkdir(mode=0o700)
        executable = executables / "codex-acp"
        executable.write_text(
            "#!/bin/sh\nexec " + shlex.quote(sys.executable) + " "
            + shlex.quote(str(SCRIPT)) + ' --codex-acp "$@"\n'
        )
        executable.chmod(0o700)

        def start(name, **settings):
            home = root / name
            home.mkdir(mode=0o700)
            homes.append(home)
            (home / ".codex").mkdir(mode=0o700)
            for filename in ("auth.json", "auth.lock"):
                (home / ".codex" / filename).symlink_to(profile / filename)
            # Do not inherit real credentials, config, or other host state.
            environment = {
                "HOME": str(home), "PATH": str(executables) + os.pathsep + os.defpath,
                "FAKE_AUTH_LEDGER": str(ledger), "PYTHONDONTWRITEBYTECODE": "1",
                "FABRO_CODEX_OAUTH_PROFILE": "1",
            }
            environment.update(settings)
            process = subprocess.Popen(
                ["sh", str(WRAPPER), "--session", name], env=environment,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                start_new_session=True,
            )
            processes.append(process)
            return process, home

        def finish(process, expected=0):
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == expected, (
                f"wrapper exit {process.returncode}, expected {expected}: {stderr}"
            )
            assert not stdout, "credential material leaked on stdout"
            assert "synthetic-refresh-" not in stderr, "credential material leaked on stderr"
            return stderr

        def retained_rotation():
            state = json.loads(ledger.read_text())
            assert json.loads(auth_path.read_text())["tokens"]["refresh_token"] == state["current"], (
                "rotated credential was overwritten by a stale snapshot"
            )
            assert auth_path.stat().st_ino == inode, "credential bind inode was replaced"

        try:
            first, _ = start("sequential-first")
            finish(first)
            retained_rotation()
            # The issuer independently proves the initial nonce is now unusable.
            replay = subprocess.run(
                [sys.executable, str(SCRIPT), "--authenticator"],
                env={"FAKE_AUTH_LEDGER": str(ledger)}, input=INITIAL_NONCE,
                text=True, capture_output=True, timeout=5,
            )
            assert replay.returncode == 23 and replay.stderr.strip() == "already redeemed nonce"
            second, _ = start("sequential-second")
            finish(second)
            retained_rotation()

            # Old wrapper reseeds this spent token, then the independent issuer rejects it.
            stale_seed = base64.b64encode(json.dumps(initial_auth).encode()).decode()
            stale, stale_home = start("stale-snapshot", CODEX_AUTH_B64=stale_seed)
            stale_stdout, stale_stderr = stale.communicate(timeout=5)
            retained_rotation()  # deterministic old-wrapper red, before other new contracts
            assert stale.returncode != 0 and "CODEX_AUTH_B64" in stale_stderr
            assert not (stale_home / "child-started").exists(), "legacy seed reached auth consumer"
            assert not stale_stdout and stale_seed not in stale_stderr
            after_stale, _ = start("after-stale-rejection")
            finish(after_stale)

            overlapping_first, overlap_home = start(
                "overlap-first", FAKE_PAUSE_BEFORE_REDEEM="1",
            )
            await_file(overlap_home / "credential-read", overlapping_first)
            overlapping_second, other_home = start("overlap-second")
            try:
                overlapping_second.wait(timeout=0.5)
                raise AssertionError("overlapping profile session bypassed the lock")
            except subprocess.TimeoutExpired:
                pass
            assert not (other_home / "credential-read").exists(), "credential read preceded profile lock"
            (overlap_home / "release").touch()
            finish(overlapping_first)
            finish(overlapping_second)
            retained_rotation()

            failed, _ = start("child-failure", FAKE_FAIL_AFTER_ROTATION="1")
            finish(failed, 17)
            retained_rotation()
            after_failure, _ = start("after-child-failure")
            finish(after_failure)
            cancelled, cancel_home = start("child-cancellation", FAKE_CANCEL_AFTER_ROTATION="1")
            await_file(cancel_home / "rotated", cancelled)
            os.killpg(cancelled.pid, signal.SIGTERM)
            finish(cancelled, -signal.SIGTERM)
            retained_rotation()
            after_cancel, _ = start("after-child-cancellation")
            finish(after_cancel)
            retained_rotation()

            config = {"model": "synthetic-model", "model_reasoning_effort": "high",
                      "nested": {"keep": [1, 2]}, "cli_auth_credentials_store": "keyring",
                      "forced_login_method": "api"}
            configured, _ = start("native-file-config", CODEX_CONFIG=json.dumps(config), FAKE_REQUIRE_CONFIG="1")
            finish(configured)
            retained_rotation()
            for name, malformed in (("invalid-json", "sensitive-malformed{"), ("non-object", "[]")):
                rejected, rejected_home = start(name, CODEX_CONFIG=malformed)
                stdout, stderr = rejected.communicate(timeout=5)
                assert rejected.returncode != 0 and "CODEX_CONFIG" in stderr
                assert malformed not in stderr and not stdout
                assert not (rejected_home / "child-started").exists()
            for variable in ("OPENAI_API_KEY", "CODEX_API_KEY"):
                conflicting, conflicting_home = start(
                    "profile-api-conflict-" + variable, **{variable: "synthetic-api-key"}
                )
                stdout, stderr = conflicting.communicate(timeout=5)
                assert conflicting.returncode != 0 and "API key credentials" in stderr
                assert "synthetic-api-key" not in stderr and not stdout
                assert not (conflicting_home / "child-started").exists()

            assert sorted(path.name for path in profile.iterdir()) == ["auth.json", "auth.lock"]
            assert profile.stat().st_mode & 0o777 == 0o700
            assert all(path.stat().st_mode & 0o777 == 0o600 for path in profile.iterdir())
            for home in homes:
                conversation = home / ".codex" / "sessions" / "conversation"
                if conversation.exists():
                    assert conversation.read_text() == home.name
                    assert (home / ".codex" / "cache" / "private").read_text() == home.name
                    assert (home / ".codex" / "config.toml").read_text() == f'private_session = "{home.name}"\n'
            print("Codex OAuth rotation, replay rejection, serialization, failure/cancel retention and isolation passed")
        finally:
            for process in processes:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.communicate(timeout=5)


if __name__ == "__main__":
    os.umask(0o077)
    if sys.argv[1:] == ["--descendant"]:
        (Path(os.environ["HOME"]) / "descendant-ready").touch()
        signal.pause()
        sys.exit(0)
    if sys.argv[1:] == ["--authenticator"]:
        sys.exit(authenticator())
    if sys.argv[1:2] == ["--codex-acp"]:
        sys.exit(fake_codex())
    regression()
