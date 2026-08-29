#!/usr/bin/env python3
"""Fill the per-developer values in .env.

Six people each need their own `.env` with the same credentials entered
correctly (§10, §15). This does that step; it cannot create the credential —
that happens at kaggle.com/settings -> API -> Create New Token.

Kaggle now issues an **API token** string. kagglehub reads it from
`KAGGLE_API_TOKEN` at highest priority; the older `KAGGLE_USERNAME`/
`KAGGLE_KEY` pair from a downloaded `kaggle.json` still works and is treated as
legacy by kagglehub itself. Both paths are supported here.

    uv run python scripts/bootstrap_env.py                  # prompts for the token
    uv run python scripts/bootstrap_env.py --name cheanwei  # also set dbt target
    uv run python scripts/bootstrap_env.py --token-file ~/token.txt
    uv run python scripts/bootstrap_env.py ~/Downloads/kaggle.json   # legacy

The token is never accepted as a command-line argument: that would put a
credential in your shell history. It is prompted for without echo, or read
from a file.

Idempotent, and it never prints a secret. Values already set are left alone
unless --force is passed, so re-running cannot clobber a working .env.
"""

from __future__ import annotations

import argparse
import getpass
import json
import select
import shutil
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ENV = REPO / ".env"
EXAMPLE = REPO / ".env.example"

# Where a legacy kaggle.json download tends to land.
SEARCH = [
    Path.home() / ".kaggle" / "kaggle.json",
    Path.home() / "Downloads" / "kaggle.json",
    REPO / "kaggle.json",
]

# Values in .env.example that mean "not filled in yet".
PLACEHOLDERS = {
    "your-kaggle-api-token",
    "your-kaggle-username",
    "your-kaggle-api-key",
    "your-gcp-project-id",
    "olist-raw-your-gcp-project-id",
    "/absolute/path/to/service-account.json",
    "dbt_your_name",
    "",
}

# Lifetime requested for a minted token. kagglesdk's own default
# is 12 hours (DEFAULT_ACCESS_TOKEN_EXPIRATION); 30 days means a token survives
# the whole project rather than needing a re-mint every working day. Kaggle may
# clamp this server-side — the value here is what we ask for, not a guarantee.
DEFAULT_EXPIRATION = "30d"

# Blank is expected here — an unset DAGSTER_HOME just means run history is
# thrown away on process exit (§8). Not worth reporting.
OPTIONAL = {
    "DAGSTER_HOME",
}


def fail(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(1)


def find_kaggle_json(explicit: str | None) -> Path | None:
    """Locate a legacy kaggle.json.

    An explicit path that does not exist is an error — the user named a file
    and got it wrong. Finding nothing during the automatic search is not: the
    modern flow has no such file at all.
    """
    if explicit:
        p = Path(explicit).expanduser()
        if not p.is_file():
            fail(f"{p} does not exist")
        return p
    for p in SEARCH:
        if p.is_file():
            return p
    return None


def read_legacy_credentials(path: Path) -> tuple[str, str]:
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        fail(f"{path} is not valid JSON: {e}")
    try:
        return data["username"], data["key"]
    except (KeyError, TypeError):
        fail(f'{path} has no "username"/"key" pair — is it really a Kaggle token?')
    raise AssertionError


def read_token(token_file: str | None, prompt: bool) -> str | None:
    """Read the API token from a file, from piped stdin, or by prompting.

    Three sources, because not every environment gives us a terminal: a
    wrapper shell, an IDE console, or a CI runner may hand us a stdin that is
    not a TTY. Silently skipping the prompt there is worse than useless — it
    looks like the script simply refused to work.
    """
    if token_file:
        p = Path(token_file).expanduser()
        if not p.is_file():
            fail(f"{p} does not exist")
        token = p.read_text().strip()
        if not token:
            fail(f"{p} is empty")
        return token

    if not prompt:
        return None

    # Piped in: `echo $TOKEN | ...` or `... < token.txt`.
    #
    # Never a bare sys.stdin.read(): when stdin is an open pipe that nobody
    # writes to — a wrapper shell, an IDE console — that blocks forever, and a
    # setup helper that hangs is worse than one that does nothing. select()
    # with a short timeout reads real piped input and gives up otherwise.
    if not sys.stdin.isatty():
        try:
            ready, _, _ = select.select([sys.stdin], [], [], 0.3)
        except (OSError, ValueError):
            return None
        if not ready:
            return None
        piped = sys.stdin.readline().strip()
        return piped or None

    print("Paste your Kaggle API token (input hidden; blank to skip).")
    print("  Get one at kaggle.com/settings -> API -> Create New Token")
    try:
        token = getpass.getpass("  token: ").strip()
    except (EOFError, OSError):
        # No usable terminal for a hidden prompt.
        return None
    return token or None


def can_mint() -> bool:
    """Whether the OAuth flow could actually run here.

    Needs the CLI and a real terminal — the login step prints a URL and then
    reads a verification code back from stdin.
    """
    return shutil.which("kaggle") is not None and sys.stdin.isatty()


def parse_duration(text: str) -> timedelta:
    """Turn "30d" / "12h" / "4w" into a timedelta.

    The Kaggle CLI has its own parser and it is broken in 2.2.4:
    `relativedelta(**{duration_str[-1]: int(duration_str[:-1])})` passes the
    suffix *letter* as the keyword, so `days=` arrives as `d=` and every value
    raises TypeError — including the `6h` in Kaggle's own documentation. The
    SDK underneath takes a plain timedelta, so we parse it ourselves and skip
    the CLI flag entirely.
    """
    units = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}
    text = text.strip().lower()
    if len(text) < 2 or text[-1] not in units or not text[:-1].isdigit():
        fail(
            f"invalid duration {text!r} — use a number and one of s, m, h, d, w"
            "\n       e.g. 12h, 30d, 4w"
        )
    return timedelta(**{units[text[-1]]: int(text[:-1])})


def mint_token_via_oauth(expiration: str) -> str | None:
    """Create a fresh Kaggle token from the terminal via the CLI's OAuth flow.

    Two steps. `kaggle auth login` is interactive — it prints a URL you approve
    in a browser — and runs with inherited stdio so you can actually answer it.
    `kaggle auth print-access-token` is then non-interactive and emits the
    token on stdout.

    **These tokens expire.** We request DEFAULT_EXPIRATION (30 days) rather
    than accepting kagglesdk's 12-hour default, so a token lasts the length of
    the project. Kaggle may clamp the request server-side. For CI, still prefer
    a durable token from kaggle.com/settings as the KAGGLE_API_TOKEN secret —
    a token that expires mid-project is a nightly run that fails at 02:00 (§8).
    """
    kaggle = shutil.which("kaggle")
    if kaggle is None:
        fail(
            "the kaggle CLI is not installed.\n"
            "       Run `uv sync` to install the dev group, then try again."
        )

    # The login flow prints a URL and then waits for you to type back a
    # verification code. Without a terminal that read fails and the CLI dies
    # with a traceback, so refuse up front and say why.
    if not sys.stdin.isatty():
        fail(
            "--oauth needs an interactive terminal: after you approve the URL,"
            "\n       Kaggle gives you a verification code to type back.\n"
            "\n       Run it directly in your terminal, not through a wrapper. Or skip"
            "\n       OAuth entirely and use a durable token from kaggle.com/settings:"
            "\n         echo '<token>' | uv run python scripts/bootstrap_env.py"
        )

    # Probe login state with the plain command — it takes no arguments and so
    # avoids the broken --expiration path described in parse_duration().
    probe = [kaggle, "auth", "print-access-token"]
    result = subprocess.run(probe, capture_output=True, text=True)
    if result.returncode != 0:
        print("Logging in to Kaggle. Approve the URL below in your browser.\n")
        login = subprocess.run([kaggle, "auth", "login", "--no-launch-browser"])
        if login.returncode != 0:
            fail("`kaggle auth login` failed; not writing anything to .env")
        result = subprocess.run(probe, capture_output=True, text=True)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip().splitlines()
            fail("login appeared to succeed but no token could be read: "
                 + (detail[-1] if detail else "unknown error"))

    # Mint with the requested lifetime through the SDK, since the CLI flag is
    # unusable. Imported here rather than at module scope: importing the kaggle
    # package prints an authentication banner.
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        from kagglesdk.kaggle_creds import KaggleCredentials

        api = KaggleApi()
        with api.build_kaggle_client() as client:
            creds = KaggleCredentials.load(client=client)
            if creds is None:
                fail("no Kaggle credentials after login; not writing anything to .env")
            response = creds.generate_access_token(parse_duration(expiration))
            if response is None or not response.token:
                fail("Kaggle returned no token; not writing anything to .env")
            return response.token
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001 - the SDK is internal API; degrade, don't crash
        print(
            f"warning: could not mint a {expiration} token via the SDK ({e}).\n"
            "         Falling back to the CLI default lifetime of 12 hours."
        )
        token = result.stdout.strip()
        return token or None


def assert_env_is_ignored() -> None:
    """Refuse to write secrets into a file git would track."""
    try:
        r = subprocess.run(
            ["git", "check-ignore", "-q", str(ENV)],
            cwd=REPO,
            capture_output=True,
        )
    except FileNotFoundError:
        print("warning: git not found; cannot confirm .env is ignored")
        return
    if r.returncode != 0:
        fail(
            ".env is NOT gitignored — refusing to write credentials into a tracked "
            "file.\n       Fix .gitignore first (§10)."
        )


def set_var(lines: list[str], key: str, value: str, force: bool) -> tuple[list[str], str]:
    """Replace `key=...` in place. Returns (lines, outcome)."""
    prefix = f"{key}="
    for i, line in enumerate(lines):
        if line.startswith(prefix):
            current = line[len(prefix):].strip()
            if current and current not in PLACEHOLDERS and not force:
                return lines, "kept (already set; --force to overwrite)"
            lines[i] = f"{prefix}{value}"
            return lines, "set"
    lines.append(f"{prefix}{value}")
    return lines, "appended"


def report_unset(lines: list[str]) -> list[str]:
    """Names of variables still sitting at their .env.example placeholder."""
    unset = []
    for line in lines:
        if line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if key in OPTIONAL:
            continue
        if value in PLACEHOLDERS:
            unset.append(key)
    return unset


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("kaggle_json", nargs="?", help="path to a legacy kaggle.json")
    ap.add_argument("--token-file", help="file containing the Kaggle API token")
    ap.add_argument("--name", help="your short name, used for DBT_DEV_DATASET (§10)")
    ap.add_argument("--force", action="store_true", help="overwrite values already set")
    ap.add_argument("--no-prompt", action="store_true", help="never prompt for a token")
    ap.add_argument(
        "--oauth",
        action="store_true",
        help="force minting via `kaggle auth login`, even without a terminal check",
    )
    ap.add_argument(
        "--no-oauth",
        action="store_true",
        help="never mint; paste or pipe a token instead",
    )
    ap.add_argument(
        "--expiration",
        default=DEFAULT_EXPIRATION,
        help=f"token lifetime for --oauth, e.g. 12h, 7d, 4w (default: {DEFAULT_EXPIRATION})",
    )
    args = ap.parse_args()

    if not ENV.exists():
        if not EXAMPLE.exists():
            fail(".env.example is missing; cannot create .env")
        ENV.write_text(EXAMPLE.read_text())
        print("created .env from .env.example")

    assert_env_is_ignored()

    lines = ENV.read_text().splitlines()
    results: list[tuple[str, str]] = []

    # Legacy path first, only when the user pointed at a file or one exists.
    legacy = None if args.oauth else find_kaggle_json(args.kaggle_json)
    token = None
    minted = False
    if legacy is None:
        # A token you supplied always wins: --token-file, or piped stdin.
        token = read_token(args.token_file, prompt=False)

        if token is None:
            # Nothing supplied. Minting one is the whole point of the script,
            # so do that by default rather than making the user discover a
            # flag — but only where the flow can actually complete.
            if args.oauth or (can_mint() and not args.no_oauth):
                token = mint_token_via_oauth(args.expiration)
                minted = token is not None
            elif not args.no_prompt:
                token = read_token(None, prompt=True)

    if legacy is not None:
        username, key = read_legacy_credentials(legacy)
        lines, r = set_var(lines, "KAGGLE_USERNAME", username, args.force)
        results.append(("KAGGLE_USERNAME", r))
        lines, r = set_var(lines, "KAGGLE_KEY", key, args.force)
        results.append(("KAGGLE_KEY", r))
    elif token:
        lines, r = set_var(lines, "KAGGLE_API_TOKEN", token, args.force)
        results.append(("KAGGLE_API_TOKEN", r))

    if args.name:
        # Per-developer dbt dataset, so concurrent work never collides in the
        # shared olist_marts (§10).
        safe = "".join(c if c.isalnum() else "_" for c in args.name.lower())
        lines, r = set_var(lines, "DBT_DEV_DATASET", f"dbt_{safe}", args.force)
        results.append(("DBT_DEV_DATASET", r))

    if not results:
        fail(
            "nothing to do — no credential supplied and no --name given.\n"
            "\n       In a real terminal this mints one for you automatically:"
            "\n         uv run python scripts/bootstrap_env.py --name <you>\n"
            "\n       It did not here because stdin is not a TTY, so the OAuth"
            "\n       verification-code step could not run. Options:\n"
            "\n         echo '<token>' | uv run python scripts/bootstrap_env.py"
            "\n         uv run python scripts/bootstrap_env.py --token-file tok.txt\n"
            "\n       Tokens come from kaggle.com/settings -> API -> Create New Token."
            "\n       Or paste one into .env directly as KAGGLE_API_TOKEN=<token>."
        )

    ENV.write_text("\n".join(lines) + "\n")

    if legacy is not None:
        print(f"read legacy credentials from {legacy}")
    for k, r in results:
        print(f"  {k:<18} {r}")

    if legacy is not None and legacy != Path.home() / ".kaggle" / "kaggle.json":
        print(
            f"\nreminder: delete {legacy} once .env is working — a token sitting in "
            "Downloads is still a token."
        )

    if minted:
        print(
            f"\nrequested a {args.expiration} token (Kaggle may clamp this). Re-run"
            "\n  --oauth when it lapses. For CI, prefer a durable token from"
            "\n  kaggle.com/settings as the KAGGLE_API_TOKEN secret (§8)."
        )

    unset = report_unset(ENV.read_text().splitlines())
    if unset:
        print("\nstill unset in .env: " + ", ".join(unset))
        if "KAGGLE_API_TOKEN" in unset:
            print(
                "  KAGGLE_API_TOKEN blocks ingestion (§4). Supply it with:"
                "\n    echo '<token>' | uv run python scripts/bootstrap_env.py"
            )
    else:
        print("\n.env is fully populated.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
