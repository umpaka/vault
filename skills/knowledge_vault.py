"""Knarr skill: knowledge-vault

A file-system-backed knowledge base for agents. Every document is a Markdown
file with optional YAML frontmatter. The agent writes structured data, the
human opens the folder in Obsidian / VS Code and sees a beautiful wiki.

Actions:
    write      — Create/update a file (full overwrite with frontmatter merge)
    append     — Append content to an existing file without rewriting
    read       — Read a file with parsed metadata
    list       — List files in a directory with metadata summaries
    search     — Search within the current vault (semantic + text fallback)
    semantic_search — Dedicated semantic similarity search
    search_all — Search across ALL vaults (semantic + text fallback)
    query      — Filter files by frontmatter (supports sort & limit)
    stats      — Dashboard: counts by type, status, recent activity
    links      — Wiki-link graph: outgoing [[links]] and backlinks to a file
    upload     — Upload a binary file (base64 or URL) with auto-created sidecar
    download   — Download a binary file (returns base64)
    delete     — Remove a file (or binary + its sidecar)

Multi-vault support:
    By default all channels share one vault ("default"). To isolate vaults:
    1. Pass vault_name="sales" → routes to VAULT_ROOT/sales/
    2. Or set VAULT_CHANNEL_MAP="-100123:sales,-100456:personal"
    3. If neither is set, everything goes to VAULT_ROOT/default/

Human features:
    - Webhook notifications on high-value writes (VAULT_NOTIFY_*)
    - File locking for concurrent write safety

Environment:
    VAULT_ROOT              — Parent directory for all vaults (default: /opt/knarr-vault)
    VAULT_CHANNEL_MAP       — Optional comma-separated chat_id:vault_name pairs
    VAULT_NOTIFY_BOT_TOKEN  — Telegram bot token for webhook notifications
    VAULT_NOTIFY_CHAT_ID    — Telegram chat ID to send notifications to
    VAULT_NOTIFY_RULES      — Semicolon-separated rules, e.g. "value>5000;status=closed-won"
"""

import base64
import fcntl
import json
import logging
import mimetypes
import os
import re
import subprocess
import time
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("knowledge-vault")

# ── Config ──────────────────────────────────────────────────────────

_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

VAULT_ROOT = Path(os.environ.get("VAULT_ROOT", "/opt/knarr-vault")).resolve()
VAULT_QUOTA_BYTES = int(os.environ.get("VAULT_QUOTA_BYTES", str(100 * 1024 * 1024)))  # 100 MB
VAULT_QUOTA_DOCS = int(os.environ.get("VAULT_QUOTA_DOCS", "1000"))
VAULT_MAX_UPLOAD_BYTES = int(os.environ.get("VAULT_MAX_UPLOAD_BYTES", str(10 * 1024 * 1024)))  # 10 MB

_ALLOWED_BINARY_EXT = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg",
    ".pdf",
    ".csv", ".json", ".xml", ".txt",
    ".xlsx", ".xls",
    ".zip", ".tar", ".gz",
    ".mp3", ".wav", ".ogg",
    ".mp4", ".webm",
}

# ── Own node identity (lazy-loaded) ─────────────────────────────────

_OWN_NODE_ID: str | None = None  # populated on first handle() call


def _get_own_node_id() -> str:
    """Return our own Knarr node ID (cached after first fetch)."""
    global _OWN_NODE_ID
    if _OWN_NODE_ID is not None:
        return _OWN_NODE_ID

    # Try env var first (set by start script or knarr.toml)
    env_id = os.environ.get("KNARR_NODE_ID", "").strip()
    if env_id:
        _OWN_NODE_ID = env_id
        log.info("Own node ID from env: %s…", env_id[:16])
        return _OWN_NODE_ID

    # Fetch from Cockpit API — try HTTPS first (knarr cockpit uses TLS), then HTTP
    api_token = os.environ.get("KNARR_API_TOKEN", "")
    for api_url in [
        os.environ.get("KNARR_API_URL", "").rstrip("/"),
        "https://127.0.0.1:8080",
        "http://127.0.0.1:8080",
    ]:
        if not api_url:
            continue
        try:
            import ssl
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            req = urllib.request.Request(f"{api_url}/api/status")
            if api_token:
                req.add_header("Authorization", f"Bearer {api_token}")
            with urllib.request.urlopen(req, timeout=5, context=ctx) as resp:
                data = json.loads(resp.read().decode())
            node_id = data.get("node_id", "")
            if node_id:
                _OWN_NODE_ID = node_id
                log.info("Own node ID from API (%s): %s…", api_url, node_id[:16])
                return _OWN_NODE_ID
        except Exception as e:
            log.debug("API fetch failed (%s): %s", api_url, e)

    # Fail-closed: unknown identity. Do NOT use empty string — that would make the
    # (not own_id) clause in is_local grant full access to all callers.
    # Use a sentinel so foreign callers with a real _caller_node_id are correctly
    # treated as foreign.
    _OWN_NODE_ID = "__UNKNOWN__"
    log.warning(
        "Could not determine own node_id from env or API — "
        "foreign callers will be scoped to their own namespaced vaults. "
        "Set KNARR_NODE_ID env var to resolve this."
    )
    return _OWN_NODE_ID


# ── Multi-vault routing ──────────────────────────────────────────────

_CHANNEL_MAP: dict[str, str] = {}
_raw_map = os.environ.get("VAULT_CHANNEL_MAP", "")
if _raw_map:
    for entry in _raw_map.split(","):
        entry = entry.strip()
        if ":" in entry:
            cid, vname = entry.split(":", 1)
            _CHANNEL_MAP[cid.strip()] = vname.strip()
    if _CHANNEL_MAP:
        log.info("Vault channel map: %s", _CHANNEL_MAP)

# Write actions that modify vault content
_WRITE_ACTIONS = {"write", "append", "update_meta", "delete", "move", "upload"}


def _resolve_vault(input_data: dict) -> tuple[Path, bool]:
    """Determine which vault directory to use for this call.

    Returns (vault_dir, is_local).
    - is_local=True  → full trust, backward compatible (our own bot / chats)
    - is_local=False → foreign network caller, scoped under node-{prefix}/
    """
    caller = input_data.get("_caller_node_id", "")
    own_id = _get_own_node_id()
    # Fail-closed: treat as local ONLY if caller is absent (direct/test call bypassing
    # knarr routing) OR if caller identity matches own node.
    # Deliberately omit the (not own_id) clause — that caused the original infosec bug
    # where an unresolvable own_id made every foreign caller appear local.
    is_local = (not caller) or (own_id not in ("", "__UNKNOWN__") and caller == own_id)

    if is_local:
        # Existing logic: VAULT_CHANNEL_MAP → vault_name → "default"
        vault_name = input_data.get("vault_name", "").strip()
        if not vault_name:
            chat_id = input_data.get("chat_id", "").strip()
            if chat_id and chat_id in _CHANNEL_MAP:
                vault_name = _CHANNEL_MAP[chat_id]
        if not vault_name:
            vault_name = "default"
        vault_name = re.sub(r"[^a-zA-Z0-9_\-]", "", vault_name) or "default"
        vault_dir = (VAULT_ROOT / vault_name).resolve()
        if not str(vault_dir).startswith(str(VAULT_ROOT)):
            vault_dir = VAULT_ROOT / "default"
        return vault_dir, True

    # Foreign caller — namespace under their node identity
    prefix = caller[:16]
    sub_name = input_data.get("vault_name", "").strip()
    sub_name = re.sub(r"[^a-zA-Z0-9_\-]", "", sub_name) or "default"
    vault_dir = (VAULT_ROOT / f"node-{prefix}" / sub_name).resolve()
    if not str(vault_dir).startswith(str(VAULT_ROOT)):
        vault_dir = VAULT_ROOT / f"node-{prefix}" / "default"
    return vault_dir, False


def _init_vault_git(vault_dir: Path):
    """Initialize a git repo in a vault directory if one doesn't exist."""
    if (vault_dir / ".git").is_dir():
        return
    try:
        env = _git_env()
        subprocess.run(["git", "init"], cwd=vault_dir, capture_output=True, env=env)
        subprocess.run(["git", "add", "-A"], cwd=vault_dir, capture_output=True, env=env)
        subprocess.run(["git", "commit", "-m", "init vault", "--allow-empty"],
                       cwd=vault_dir, capture_output=True, env=env, timeout=10)
        log.info("Initialized git repo for vault: %s", vault_dir.name)
    except Exception as e:
        log.warning("Failed to init git for vault %s: %s", vault_dir.name, e)


# ── Vault metadata / ACL ────────────────────────────────────────────

def _load_vault_meta(vault_dir: Path) -> dict:
    """Read .vault.json from a vault directory. Returns {} if missing."""
    meta_path = vault_dir / ".vault.json"
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("Failed to read %s: %s", meta_path, e)
        return {}


def _save_vault_meta(vault_dir: Path, meta: dict):
    """Write .vault.json with advisory file lock."""
    meta_path = vault_dir / ".vault.json"
    lock_path = meta_path.with_suffix(".json.lock")
    vault_dir.mkdir(parents=True, exist_ok=True)

    fd = None
    try:
        fd = open(lock_path, "w")
        deadline = time.monotonic() + 3.0
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (IOError, OSError):
                if time.monotonic() >= deadline:
                    raise TimeoutError("Could not lock .vault.json")
                time.sleep(0.05)

        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    finally:
        if fd:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
                fd.close()
            except Exception:
                pass
            try:
                lock_path.unlink(missing_ok=True)
            except Exception:
                pass


def _ensure_vault_meta(vault_dir: Path, caller_node_id: str):
    """Create .vault.json for a foreign caller's vault if it doesn't exist."""
    meta_path = vault_dir / ".vault.json"
    if meta_path.exists():
        return
    meta = {
        "owner": caller_node_id,
        "created": datetime.now(timezone.utc).isoformat(),
        "visibility": "private",
        "shared_with": {},
        "quota_bytes": VAULT_QUOTA_BYTES,
        "quota_docs": VAULT_QUOTA_DOCS,
        "used_bytes": 0,
    }
    _save_vault_meta(vault_dir, meta)
    log.info("Created vault meta for foreign node %s… in %s",
             caller_node_id[:16], vault_dir.name)


def _meta_allows(meta: dict, caller_node_id: str, write: bool = False) -> bool:
    """Check if vault metadata grants access to a caller (no is_local check)."""
    if not meta:
        return False
    # Owner always has full access
    if meta.get("owner") == caller_node_id:
        return True
    # Check shared_with
    shared = meta.get("shared_with", {})
    if caller_node_id in shared:
        perm = shared[caller_node_id]
        if perm == "write":
            return True
        if perm == "read" and not write:
            return True
    # Check visibility
    vis = meta.get("visibility", "private")
    if vis == "public_write":
        return True
    if vis == "public_read" and not write:
        return True
    return False


def _check_access(vault_dir: Path, caller_node_id: str,
                  is_local: bool, write: bool = False) -> bool:
    """Check if a caller has access to a vault.

    Local callers always have full access. Foreign callers are checked
    against .vault.json ACL.
    """
    if is_local:
        return True
    meta = _load_vault_meta(vault_dir)
    if not meta:
        # No .vault.json → legacy local vault → deny foreign access
        return False
    return _meta_allows(meta, caller_node_id, write=write)


def _check_quota(vault_dir: Path) -> tuple[bool, str]:
    """Check if a foreign vault is over quota. Returns (is_over, message)."""
    meta = _load_vault_meta(vault_dir)
    if not meta:
        return False, ""
    # Bytes quota
    quota_bytes = meta.get("quota_bytes", VAULT_QUOTA_BYTES)
    used_bytes = meta.get("used_bytes", 0)
    if used_bytes >= quota_bytes:
        return True, (
            f"Storage quota exceeded: {used_bytes:,} / {quota_bytes:,} bytes. "
            f"Delete files to free space or contact the vault owner."
        )
    # Document count quota
    quota_docs = meta.get("quota_docs", VAULT_QUOTA_DOCS)
    doc_count = sum(1 for f in vault_dir.rglob("*.md")
                    if not f.name.startswith(".") and not f.name.startswith("_"))
    if doc_count >= quota_docs:
        return True, (
            f"Document quota exceeded: {doc_count:,} / {quota_docs:,} documents. "
            f"Delete files to free space or contact the vault owner."
        )
    return False, ""


def _update_used_bytes(vault_dir: Path):
    """Recalculate and store used_bytes in .vault.json."""
    meta = _load_vault_meta(vault_dir)
    if not meta:
        return
    total = 0
    for f in vault_dir.rglob("*"):
        if f.is_file() and f.name != ".vault.json" and not f.name.endswith(".lock"):
            try:
                total += f.stat().st_size
            except OSError:
                pass
    meta["used_bytes"] = total
    _save_vault_meta(vault_dir, meta)


# ── Webhook / notification config ────────────────────────────────────

_NOTIFY_BOT_TOKEN = os.environ.get("VAULT_NOTIFY_BOT_TOKEN", "")
_NOTIFY_CHAT_ID = os.environ.get("VAULT_NOTIFY_CHAT_ID", "")
_NOTIFY_RULES: list[tuple[str, str, str]] = []  # [(field, operator, value), ...]

_raw_rules = os.environ.get("VAULT_NOTIFY_RULES", "")
if _raw_rules:
    for rule in _raw_rules.split(";"):
        rule = rule.strip()
        if not rule:
            continue
        # Parse: "value>5000", "status=closed-won", "value>=1000"
        for op in (">=", "<=", ">", "<", "="):
            if op in rule:
                field, val = rule.split(op, 1)
                _NOTIFY_RULES.append((field.strip(), op, val.strip()))
                break
    if _NOTIFY_RULES:
        log.info("Vault notify rules: %s", _NOTIFY_RULES)


def _check_notify(meta: dict, rel_path: str, vault_name: str, action: str):
    """Check if a write matches notification rules and send alert."""
    if not _NOTIFY_BOT_TOKEN or not _NOTIFY_CHAT_ID or not _NOTIFY_RULES:
        return

    triggered = []
    for field, op, threshold in _NOTIFY_RULES:
        file_val = meta.get(field)
        if file_val is None:
            continue
        try:
            if op in (">", ">=", "<", "<="):
                fv = float(file_val) if not isinstance(file_val, (int, float)) else file_val
                tv = float(threshold)
                if op == ">" and fv > tv:
                    triggered.append(f"{field}={file_val} (>{threshold})")
                elif op == ">=" and fv >= tv:
                    triggered.append(f"{field}={file_val} (>={threshold})")
                elif op == "<" and fv < tv:
                    triggered.append(f"{field}={file_val} (<{threshold})")
                elif op == "<=" and fv <= tv:
                    triggered.append(f"{field}={file_val} (<={threshold})")
            elif op == "=":
                if str(file_val).lower() == threshold.lower():
                    triggered.append(f"{field}={file_val}")
        except (ValueError, TypeError):
            continue

    if not triggered:
        return

    # Build notification message
    title = meta.get("company") or meta.get("topic") or meta.get("type", "file")
    msg = (
        f"🔔 *Vault Alert* [{action}]\n\n"
        f"**{rel_path}** in vault `{vault_name}`\n"
        f"Title: {title}\n"
        f"Triggered: {', '.join(triggered)}\n"
    )
    # Add key metadata
    for k in ("type", "status", "value", "company", "contact"):
        if k in meta and k not in ("type",):
            msg += f"{k}: {meta[k]}\n"

    _send_telegram_notification(msg)


def _send_telegram_notification(text: str):
    """Send a Telegram message via bot API."""
    if not _NOTIFY_BOT_TOKEN or not _NOTIFY_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{_NOTIFY_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": _NOTIFY_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=payload,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        log.info("Sent vault notification to chat %s", _NOTIFY_CHAT_ID)
    except Exception as e:
        log.warning("Failed to send vault notification: %s", e)


# ── Knarr-mail notification ───────────────────────────────────────────

def _send_knarr_mail(to_node: str, content: str):
    """Fire-and-forget a knarr-mail notification to another node.

    Calls the local Cockpit API to execute the knarr-mail skill.
    Failures are logged but never block the caller.
    """
    api_url = os.environ.get("KNARR_API_URL", "https://127.0.0.1:8080").rstrip("/")
    api_token = os.environ.get("KNARR_API_TOKEN", "")
    payload = json.dumps({
        "skill": "knarr-mail",
        "input": {
            "action": "send",
            "to": to_node,
            "content": content,
        },
    }).encode("utf-8")
    try:
        req = urllib.request.Request(
            f"{api_url}/api/execute",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        if api_token:
            req.add_header("Authorization", f"Bearer {api_token}")
        urllib.request.urlopen(req, timeout=10)
        log.info("Sent knarr-mail notification to node %s…", to_node[:16])
    except Exception as e:
        log.warning("Failed to send knarr-mail to %s…: %s", to_node[:16], e)


# ── Frontmatter parser (no PyYAML dependency) ───────────────────────

_FM_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def _parse_frontmatter(text: str) -> tuple[dict, str]:
    """Parse YAML-ish frontmatter from markdown text."""
    m = _FM_RE.match(text)
    if not m:
        return {}, text

    meta = {}
    for line in m.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip()
        val = val.strip()

        if (val.startswith('"') and val.endswith('"')) or \
           (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]

        if val.startswith("[") and val.endswith("]"):
            items = [v.strip().strip("'\"") for v in val[1:-1].split(",") if v.strip()]
            meta[key] = items
        elif val.replace(".", "", 1).replace("-", "", 1).isdigit():
            meta[key] = float(val) if "." in val else int(val)
        elif val.lower() in ("true", "yes"):
            meta[key] = True
        elif val.lower() in ("false", "no"):
            meta[key] = False
        else:
            meta[key] = val

    body = text[m.end():]
    return meta, body


def _serialize_frontmatter(meta: dict) -> str:
    """Serialize a dict back to YAML frontmatter string."""
    lines = ["---"]
    for k, v in meta.items():
        if isinstance(v, list):
            lines.append(f"{k}: [{', '.join(str(i) for i in v)}]")
        elif isinstance(v, bool):
            lines.append(f"{k}: {'true' if v else 'false'}")
        elif isinstance(v, str) and (":" in v or "\n" in v):
            lines.append(f'{k}: "{v}"')
        else:
            lines.append(f"{k}: {v}")
    lines.append("---")
    return "\n".join(lines) + "\n"


# ── Path security ────────────────────────────────────────────────────

def _safe_path(relative: str, vault_dir: Path) -> Path | None:
    """Resolve a relative path within a vault. Returns None if it escapes."""
    relative = relative.strip().lstrip("/\\")
    if not relative:
        return None
    if not relative.endswith("/") and not relative.endswith(".md"):
        relative += ".md"
    target = (vault_dir / relative).resolve()
    if not str(target).startswith(str(vault_dir)):
        log.warning("Path escape attempt blocked: %s", relative)
        return None
    return target


def _safe_path_binary(relative: str, vault_dir: Path) -> Path | None:
    """Resolve a relative path for a binary file. Does NOT force .md extension.
    Validates that the extension is in the allowed set."""
    relative = relative.strip().lstrip("/\\")
    if not relative:
        return None
    target = (vault_dir / relative).resolve()
    if not str(target).startswith(str(vault_dir)):
        log.warning("Path escape attempt blocked: %s", relative)
        return None
    ext = target.suffix.lower()
    if ext not in _ALLOWED_BINARY_EXT:
        return None
    return target


# ── Git integration ──────────────────────────────────────────────────

def _git_env() -> dict:
    return {**os.environ, "GIT_AUTHOR_NAME": "knarr-vault",
            "GIT_AUTHOR_EMAIL": "vault@knarr.local",
            "GIT_COMMITTER_NAME": "knarr-vault",
            "GIT_COMMITTER_EMAIL": "vault@knarr.local"}


def _git_available(vault_dir: Path) -> bool:
    return (vault_dir / ".git").is_dir()


def _git_commit(message: str, vault_dir: Path, paths: list[str] | None = None):
    if not _git_available(vault_dir):
        return
    try:
        env = _git_env()
        if paths:
            subprocess.run(["git", "add"] + paths,
                           cwd=vault_dir, capture_output=True, env=env)
        else:
            subprocess.run(["git", "add", "-A"],
                           cwd=vault_dir, capture_output=True, env=env)
        subprocess.run(
            ["git", "commit", "-m", message, "--allow-empty"],
            cwd=vault_dir, capture_output=True, env=env, timeout=10,
        )
    except Exception as e:
        log.warning("Git commit failed: %s", e)


# ── File locking (conflict resolution) ──────────────────────────────

class _FileLock:
    """Simple advisory file lock using fcntl. Context manager.

    Creates a .lock file next to the target. Blocks up to `timeout` seconds
    waiting for the lock, then raises TimeoutError.
    """

    def __init__(self, target: Path, timeout: float = 5.0):
        self.lock_path = target.with_suffix(target.suffix + ".lock")
        self.timeout = timeout
        self._fd = None

    def __enter__(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = open(self.lock_path, "w")
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except (IOError, OSError):
                if time.monotonic() >= deadline:
                    self._fd.close()
                    raise TimeoutError(
                        f"Could not acquire lock on {self.lock_path.name} "
                        f"within {self.timeout}s — another write may be in progress"
                    )
                time.sleep(0.05)

    def __exit__(self, *args):
        if self._fd:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                self._fd.close()
            except Exception:
                pass
            try:
                self.lock_path.unlink(missing_ok=True)
            except Exception:
                pass


# ── Wiki-link parser ─────────────────────────────────────────────────

_WIKILINK_RE = re.compile(r"\[\[([^\]|]+?)(?:\|[^\]]*?)?\]\]")


def _extract_wikilinks(text: str) -> list[str]:
    """Extract [[wiki-link]] targets from text. Supports [[target|display]]."""
    return list(dict.fromkeys(_WIKILINK_RE.findall(text)))  # unique, ordered


# ── Sorting / filtering helpers ──────────────────────────────────────

def _parse_sort(sort_expr: str) -> tuple[str, bool] | None:
    """Parse sort expression like 'value:desc' or 'updated:asc'."""
    if not sort_expr or not sort_expr.strip():
        return None
    parts = sort_expr.strip().split(":")
    field = parts[0].strip()
    descending = len(parts) > 1 and parts[1].strip().lower() in ("desc", "descending", "d")
    return (field, descending)


def _parse_filters(filter_expr: str) -> dict[str, str]:
    """Parse comma-separated filter expressions."""
    filters = {}
    if not filter_expr:
        return filters
    for part in filter_expr.split(","):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            filters[k.strip()] = v.strip()
    return filters


def _matches_filters(meta: dict, filters: dict[str, str]) -> bool:
    """Check if a file's metadata matches all filters."""
    for fk, fv in filters.items():
        file_val = str(meta.get(fk, "")).lower()
        if fv.startswith(">") and fv[1:].replace(".", "").isdigit():
            try:
                if float(meta.get(fk, 0)) <= float(fv[1:]):
                    return False
            except (ValueError, TypeError):
                return False
        elif fv.startswith("<") and fv[1:].replace(".", "").isdigit():
            try:
                if float(meta.get(fk, 0)) >= float(fv[1:]):
                    return False
            except (ValueError, TypeError):
                return False
        elif file_val != fv.lower():
            list_val = meta.get(fk, [])
            if isinstance(list_val, list):
                if fv.lower() not in [str(x).lower() for x in list_val]:
                    return False
            else:
                return False
    return True


def _collect_files(target_dir: Path, vault_dir: Path, filters: dict,
                   sort: tuple[str, bool] | None = None,
                   limit: int = 0) -> list[tuple[str, dict, str]]:
    """Collect and optionally sort/limit files. Returns [(rel_path, meta, body), ...]."""
    results = []
    for md_file in sorted(target_dir.rglob("*.md")):
        if md_file.name.startswith(".") or md_file.name.startswith("_"):
            continue
        try:
            text = md_file.read_text(encoding="utf-8")
        except Exception:
            continue
        meta, body = _parse_frontmatter(text)
        rel = str(md_file.relative_to(vault_dir))
        if filters and not _matches_filters(meta, filters):
            continue
        results.append((rel, meta, body))

    # Sort
    if sort:
        sort_field, descending = sort

        def sort_key(item):
            val = item[1].get(sort_field)
            if val is None:
                return (1, "")  # nulls last
            if isinstance(val, (int, float)):
                return (0, val)
            return (0, str(val).lower())

        results.sort(key=sort_key, reverse=descending)

    # Limit
    if limit > 0:
        results = results[:limit]

    return results


# ── Zvec semantic search (optional) ──────────────────────────────────

try:
    import zvec as _zvec
    _ZVEC_AVAILABLE = True
except ImportError:
    _zvec = None  # type: ignore[assignment]
    _ZVEC_AVAILABLE = False

_EMBEDDING_MODEL = "gemini-embedding-001"
_EMBEDDING_DIM = 768


def _get_embedding(text: str) -> list[float] | None:
    """Compute a text embedding via the Gemini embedding API.

    Returns None on failure (missing key, network error, etc.).
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return None

    # Truncate to ~8000 chars to stay within token limits
    text = text[:8000].strip()
    if not text:
        return None

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/"
        f"models/{_EMBEDDING_MODEL}:embedContent?key={api_key}"
    )
    payload = json.dumps({
        "model": f"models/{_EMBEDDING_MODEL}",
        "content": {"parts": [{"text": text}]},
        "outputDimensionality": _EMBEDDING_DIM,
    }).encode("utf-8")

    try:
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        values = data.get("embedding", {}).get("values", [])
        if len(values) == _EMBEDDING_DIM:
            return values
        log.warning("Embedding returned %d dims (expected %d)", len(values), _EMBEDDING_DIM)
        return values if values else None
    except Exception as e:
        log.warning("Embedding API call failed: %s", e)
        return None


def _zvec_collection(vault_dir: Path):
    """Open or create a zvec collection for a vault. Returns None if unavailable."""
    if not _ZVEC_AVAILABLE:
        return None

    zvec_path = vault_dir / ".zvec"
    try:
        if zvec_path.exists():
            return _zvec.open(path=str(zvec_path))
        # Create new collection
        schema = _zvec.CollectionSchema(
            name="vault_docs",
            vectors=_zvec.VectorSchema("embedding", _zvec.DataType.VECTOR_FP32, _EMBEDDING_DIM),
        )
        return _zvec.create_and_open(path=str(zvec_path), schema=schema)
    except Exception as e:
        log.warning("Failed to open/create zvec collection at %s: %s", zvec_path, e)
        return None


def _zvec_upsert(vault_dir: Path, doc_id: str, text: str):
    """Compute embedding and upsert a document into the vault's zvec index."""
    if not _ZVEC_AVAILABLE:
        return
    embedding = _get_embedding(text)
    if not embedding:
        return
    collection = _zvec_collection(vault_dir)
    if collection is None:
        return
    try:
        collection.upsert([
            _zvec.Doc(id=doc_id, vectors={"embedding": embedding}),
        ])
        log.debug("Zvec upsert: %s", doc_id)
    except Exception as e:
        log.warning("Zvec upsert failed for %s: %s", doc_id, e)
    finally:
        try:
            collection.close()
        except Exception:
            pass


def _zvec_remove(vault_dir: Path, doc_id: str):
    """Remove a document from the vault's zvec index."""
    if not _ZVEC_AVAILABLE:
        return
    collection = _zvec_collection(vault_dir)
    if collection is None:
        return
    try:
        collection.delete(ids=[doc_id])
        log.debug("Zvec remove: %s", doc_id)
    except Exception as e:
        log.warning("Zvec remove failed for %s: %s", doc_id, e)
    finally:
        try:
            collection.close()
        except Exception:
            pass


def _zvec_search(vault_dir: Path, query: str, topk: int = 20) -> list[tuple[str, float]] | None:
    """Semantic search in a vault's zvec index. Returns [(doc_id, score), ...] or None."""
    if not _ZVEC_AVAILABLE:
        return None
    zvec_path = vault_dir / ".zvec"
    if not zvec_path.exists():
        return None
    embedding = _get_embedding(query)
    if not embedding:
        return None
    collection = _zvec_collection(vault_dir)
    if collection is None:
        return None
    try:
        results = collection.query(
            _zvec.VectorQuery("embedding", vector=embedding),
            topk=topk,
        )
        return [(r.id, r.score) for r in results]
    except Exception as e:
        log.warning("Zvec search failed: %s", e)
        return None
    finally:
        try:
            collection.close()
        except Exception:
            pass


# ── Actions ──────────────────────────────────────────────────────────

def _action_write(path: str, content: str, vault_dir: Path) -> dict:
    """Write or update a markdown file. Creates directories as needed."""
    target = _safe_path(path, vault_dir)
    if not target:
        return _error(f"Invalid path: {path}")

    with _FileLock(target):
        new_meta, new_body = _parse_frontmatter(content)

        if target.exists():
            old_text = target.read_text(encoding="utf-8")
            old_meta, _ = _parse_frontmatter(old_text)
            if not new_meta and old_meta:
                new_meta = old_meta
                new_meta["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        else:
            if "created" not in new_meta:
                new_meta["created"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        if "updated" not in new_meta:
            new_meta["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        if new_meta:
            final = _serialize_frontmatter(new_meta) + "\n" + new_body.lstrip("\n")
        else:
            final = content

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(final, encoding="utf-8")

    rel = str(target.relative_to(vault_dir))
    log.info("Wrote %s in vault '%s' (%d bytes)", rel, vault_dir.name, len(final))
    _git_commit(f"vault: update {rel}", vault_dir, [str(target)])

    # Update zvec semantic index
    _zvec_upsert(vault_dir, doc_id=rel, text=new_body)

    # Webhook check
    _check_notify(new_meta, rel, vault_dir.name, "write")

    return {
        "result": f"Written: {rel} ({len(final)} bytes) [vault: {vault_dir.name}]",
        "path": rel, "vault": vault_dir.name, "status": "completed",
    }


def _action_append(path: str, content: str, vault_dir: Path) -> dict:
    """Append content to an existing file without rewriting.

    If the file doesn't exist, creates it. Bumps the `updated` timestamp
    in frontmatter. The appended content is added after a blank line at
    the end of the existing body.
    """
    target = _safe_path(path, vault_dir)
    if not target:
        return _error(f"Invalid path: {path}")

    with _FileLock(target):
        if target.exists():
            existing_text = target.read_text(encoding="utf-8")
            meta, body = _parse_frontmatter(existing_text)
            meta["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            # Append with a blank line separator
            new_body = body.rstrip("\n") + "\n\n" + content.strip() + "\n"
            final = _serialize_frontmatter(meta) + "\n" + new_body
        else:
            # File doesn't exist — create with content
            new_meta, new_body = _parse_frontmatter(content)
            if "created" not in new_meta:
                new_meta["created"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if "updated" not in new_meta:
                new_meta["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if new_meta:
                final = _serialize_frontmatter(new_meta) + "\n" + new_body.lstrip("\n")
            else:
                final = content
            meta = new_meta

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(final, encoding="utf-8")

    rel = str(target.relative_to(vault_dir))
    log.info("Appended to %s in vault '%s' (%d bytes total)", rel, vault_dir.name, len(final))
    _git_commit(f"vault: append {rel}", vault_dir, [str(target)])

    # Update zvec semantic index (re-index full body after append)
    _, full_body = _parse_frontmatter(final)
    _zvec_upsert(vault_dir, doc_id=rel, text=full_body)

    _check_notify(meta, rel, vault_dir.name, "append")

    return {
        "result": f"Appended to: {rel} ({len(final)} bytes total) [vault: {vault_dir.name}]",
        "path": rel, "vault": vault_dir.name, "status": "completed",
    }


def _action_read(path: str, vault_dir: Path) -> dict:
    """Read a markdown file and return its content + parsed frontmatter."""
    target = _safe_path(path, vault_dir)
    if not target:
        return _error(f"Invalid path: {path}")
    if not target.exists():
        return _error(f"File not found: {path}")

    text = target.read_text(encoding="utf-8")
    meta, body = _parse_frontmatter(text)
    rel = str(target.relative_to(vault_dir))

    result_parts = [f"# {rel} [vault: {vault_dir.name}]"]
    if meta:
        result_parts.append("**Metadata:** " + ", ".join(f"{k}={v}" for k, v in meta.items()))
    result_parts.append("")
    result_parts.append(body.strip())

    return {
        "result": "\n".join(result_parts),
        "path": rel, "vault": vault_dir.name, "status": "completed",
    }


def _action_list(path: str, filter_expr: str, vault_dir: Path,
                 sort_expr: str = "", limit: int = 0) -> dict:
    """List files in a directory with metadata summaries.

    Supports:
      filter  — "status=active,type=lead,value>1000"
      sort    — "value:desc", "updated:asc", "company:asc"
      limit   — max number of results (0 = unlimited)
    """
    dir_path = path.strip().rstrip("/") if path.strip() else ""
    target_dir = (vault_dir / dir_path).resolve() if dir_path else vault_dir

    if not str(target_dir).startswith(str(vault_dir)):
        return _error("Invalid directory path")
    if not target_dir.exists():
        return _error(f"Directory not found: {dir_path or '/'}")

    filters = _parse_filters(filter_expr)
    sort = _parse_sort(sort_expr)
    files = _collect_files(target_dir, vault_dir, filters, sort, limit)

    if not files:
        msg = f"No files found in {dir_path or '/'}"
        if filter_expr:
            msg += f" matching {filter_expr}"
        return {"result": msg, "path": dir_path or "/", "vault": vault_dir.name, "status": "completed"}

    # Build output
    entries = []
    for rel, meta, body in files:
        parts = [f"- **{rel}**"]
        if meta:
            key_vals = []
            for k, v in meta.items():
                if k in ("created", "updated"):
                    continue
                if isinstance(v, list):
                    key_vals.append(f"{k}=[{','.join(str(x) for x in v)}]")
                else:
                    key_vals.append(f"{k}={v}")
            if key_vals:
                parts.append(" | " + ", ".join(key_vals))
        # Preview
        for bline in body.strip().splitlines():
            bline = bline.strip()
            if bline and not bline.startswith("#"):
                parts.append(f"\n  _{bline[:80]}_")
                break
        entries.append("".join(parts))

    header = f"## Vault '{vault_dir.name}': /{dir_path}" if dir_path else f"## Vault '{vault_dir.name}': /"
    if filter_expr:
        header += f" (filter: {filter_expr})"
    if sort_expr:
        header += f" (sort: {sort_expr})"
    if limit:
        header += f" (limit: {limit})"
    header += f"\n{len(entries)} file(s)\n"

    return {
        "result": header + "\n".join(entries),
        "path": dir_path or "/", "vault": vault_dir.name, "status": "completed",
    }


def _text_search(query: str, vault_dir: Path, max_results: int = 20) -> list[str]:
    """Plain-text search returning formatted match entries."""
    query_lower = query.lower()
    matches = []
    for md_file in sorted(vault_dir.rglob("*.md")):
        if md_file.name.startswith(".") or md_file.name.startswith("_"):
            continue
        try:
            text = md_file.read_text(encoding="utf-8")
        except Exception:
            continue
        if query_lower not in text.lower():
            continue
        rel = str(md_file.relative_to(vault_dir))
        meta, body = _parse_frontmatter(text)
        contexts = []
        for line in body.splitlines():
            if query_lower in line.lower():
                contexts.append(line.strip()[:120])
                if len(contexts) >= 3:
                    break
        entry = f"- **{rel}**"
        if meta.get("type"):
            entry += f" [{meta['type']}]"
        if meta.get("status"):
            entry += f" ({meta['status']})"
        for ctx in contexts:
            entry += f"\n  > {ctx}"
        matches.append(entry)
        if len(matches) >= max_results:
            break
    return matches


def _action_search(query: str, vault_dir: Path) -> dict:
    """Search within a vault. Uses semantic search if available, text fallback."""
    if not query.strip():
        return _error("Search query is required")

    # Try semantic search first
    vec_results = _zvec_search(vault_dir, query, topk=20)
    if vec_results:
        matches = []
        for doc_id, score in vec_results:
            target = vault_dir / doc_id
            if not target.exists():
                continue
            try:
                text = target.read_text(encoding="utf-8")
            except Exception:
                continue
            meta, body = _parse_frontmatter(text)
            preview = ""
            for line in body.strip().splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    preview = line[:120]
                    break
            entry = f"- **{doc_id}** (relevance: {score:.3f})"
            if meta.get("type"):
                entry += f" [{meta['type']}]"
            if meta.get("status"):
                entry += f" ({meta['status']})"
            if preview:
                entry += f"\n  > {preview}"
            matches.append(entry)
        if matches:
            return {
                "result": (
                    f"## Semantic Search: '{query}' [vault: {vault_dir.name}]\n"
                    f"{len(matches)} result(s)\n\n" + "\n".join(matches)
                ),
                "vault": vault_dir.name, "status": "completed",
            }

    # Fallback to text search
    matches = _text_search(query, vault_dir)
    if not matches:
        return {"result": f"No results for '{query}' [vault: {vault_dir.name}]",
                "vault": vault_dir.name, "status": "completed"}
    return {
        "result": f"## Search: '{query}' [vault: {vault_dir.name}]\n{len(matches)} match(es)\n\n" + "\n".join(matches),
        "vault": vault_dir.name, "status": "completed",
    }


def _action_semantic_search(query: str, vault_dir: Path) -> dict:
    """Dedicated semantic search action. Requires zvec + embeddings."""
    if not query.strip():
        return _error("Search query is required")
    if not _ZVEC_AVAILABLE:
        return _error("Semantic search unavailable: zvec not installed")
    zvec_path = vault_dir / ".zvec"
    if not zvec_path.exists():
        return _error(
            "No semantic index for this vault yet. "
            "Write some documents first — they are indexed automatically."
        )
    vec_results = _zvec_search(vault_dir, query, topk=20)
    if vec_results is None:
        return _error("Semantic search failed (embedding or index error)")
    if not vec_results:
        return {"result": f"No semantic matches for '{query}' [vault: {vault_dir.name}]",
                "vault": vault_dir.name, "status": "completed"}

    matches = []
    for doc_id, score in vec_results:
        target = vault_dir / doc_id
        if not target.exists():
            continue
        try:
            text = target.read_text(encoding="utf-8")
        except Exception:
            continue
        meta, body = _parse_frontmatter(text)
        preview = ""
        for line in body.strip().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                preview = line[:120]
                break
        entry = f"- **{doc_id}** (score: {score:.3f})"
        if meta.get("type"):
            entry += f" [{meta['type']}]"
        if meta.get("status"):
            entry += f" ({meta['status']})"
        if preview:
            entry += f"\n  > {preview}"
        matches.append(entry)

    return {
        "result": (
            f"## Semantic Search: '{query}' [vault: {vault_dir.name}]\n"
            f"{len(matches)} result(s)\n\n" + "\n".join(matches)
        ),
        "vault": vault_dir.name, "status": "completed",
    }


def _accessible_vaults(caller_node_id: str) -> list[Path]:
    """Return vault directories accessible to a given caller.

    Local callers (caller_node_id == OWN_NODE_ID or empty) see everything.
    Foreign callers see their own node-{prefix}/* vaults plus any vaults
    that have shared access with them (via .vault.json).
    """
    if not VAULT_ROOT.exists():
        return []

    own_id = _get_own_node_id()
    is_local = (not caller_node_id) or (own_id not in ("", "__UNKNOWN__") and caller_node_id == own_id)

    vaults: list[Path] = []
    for entry in sorted(VAULT_ROOT.iterdir()):
        if not entry.is_dir() or entry.name.startswith("."):
            continue

        if entry.name.startswith("node-"):
            # Nested: node-{prefix}/{sub_vault}/
            for sub in sorted(entry.iterdir()):
                if not sub.is_dir() or sub.name.startswith("."):
                    continue
                if is_local:
                    vaults.append(sub)
                else:
                    # Foreign caller: own prefix or shared
                    prefix = caller_node_id[:16]
                    if entry.name == f"node-{prefix}":
                        vaults.append(sub)
                    else:
                        meta = _load_vault_meta(sub)
                        if _meta_allows(meta, caller_node_id, write=False):
                            vaults.append(sub)
        else:
            # Top-level vault (local-only unless shared)
            if is_local:
                vaults.append(entry)
            else:
                meta = _load_vault_meta(entry)
                if _meta_allows(meta, caller_node_id, write=False):
                    vaults.append(entry)

    return vaults


def _action_search_all(d: dict, caller_node_id: str) -> dict:
    """Search across accessible vaults. Semantic first, text fallback."""
    query = (d.get("query", "") or d.get("content", "")).strip()
    if not query:
        return _error("Search query is required")

    vaults = _accessible_vaults(caller_node_id)
    searched_names = [v.name for v in vaults]
    all_matches: list[str] = []
    used_semantic = False

    # Try semantic search across all vaults
    for vault_dir in vaults:
        vec_results = _zvec_search(vault_dir, query, topk=10)
        if not vec_results:
            continue
        used_semantic = True
        vault_label = vault_dir.parent.name + "/" + vault_dir.name \
            if vault_dir.parent != VAULT_ROOT else vault_dir.name
        for doc_id, score in vec_results:
            target = vault_dir / doc_id
            if not target.exists():
                continue
            try:
                text = target.read_text(encoding="utf-8")
            except Exception:
                continue
            meta, body = _parse_frontmatter(text)
            preview = ""
            for line in body.strip().splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    preview = line[:120]
                    break
            entry = f"- **[{vault_label}] {doc_id}** (relevance: {score:.3f})"
            if meta.get("type"):
                entry += f" [{meta['type']}]"
            if meta.get("status"):
                entry += f" ({meta['status']})"
            if preview:
                entry += f"\n  > {preview}"
            all_matches.append(entry)
            if len(all_matches) >= 30:
                break
        if len(all_matches) >= 30:
            break

    # If semantic found results, return them
    if all_matches:
        search_type = "Semantic" if used_semantic else "Global"
        return {
            "result": (
                f"## {search_type} Search: '{query}'\n"
                f"Vaults searched: {', '.join(searched_names)}\n"
                f"{len(all_matches)} result(s)\n\n" + "\n".join(all_matches)
            ),
            "status": "completed",
        }

    # Fallback to text search
    query_lower = query.lower()
    for vault_dir in vaults:
        for md_file in sorted(vault_dir.rglob("*.md")):
            if md_file.name.startswith(".") or md_file.name.startswith("_"):
                continue
            try:
                text = md_file.read_text(encoding="utf-8")
            except Exception:
                continue
            if query_lower not in text.lower():
                continue
            rel = str(md_file.relative_to(vault_dir))
            meta, body = _parse_frontmatter(text)
            contexts = []
            for line in body.splitlines():
                if query_lower in line.lower():
                    contexts.append(line.strip()[:120])
                    if len(contexts) >= 2:
                        break
            vault_label = vault_dir.parent.name + "/" + vault_dir.name \
                if vault_dir.parent != VAULT_ROOT else vault_dir.name
            entry = f"- **[{vault_label}] {rel}**"
            if meta.get("type"):
                entry += f" [{meta['type']}]"
            if meta.get("status"):
                entry += f" ({meta['status']})"
            for ctx in contexts:
                entry += f"\n  > {ctx}"
            all_matches.append(entry)
            if len(all_matches) >= 30:
                break
        if len(all_matches) >= 30:
            break

    if not all_matches:
        return {"result": f"No results for '{query}' across {len(searched_names)} vault(s)",
                "status": "completed"}

    return {
        "result": (
            f"## Global Search: '{query}'\n"
            f"Vaults searched: {', '.join(searched_names)}\n"
            f"{len(all_matches)} match(es)\n\n" + "\n".join(all_matches)
        ),
        "status": "completed",
    }


def _action_list_vaults(input_data: dict, caller_node_id: str) -> dict:
    """List all vaults accessible to the caller."""
    vaults = _accessible_vaults(caller_node_id)

    if not vaults:
        return {"result": "No vaults found.", "status": "completed"}

    lines = [f"## Your Vaults ({len(vaults)})"]
    for v in vaults:
        vault_label = v.parent.name + "/" + v.name \
            if v.parent != VAULT_ROOT else v.name
        meta = _load_vault_meta(v)
        file_count = sum(1 for _ in v.rglob("*.md")
                         if not _.name.startswith(".") and not _.name.startswith("_"))
        parts = [f"- **{vault_label}** — {file_count} file(s)"]
        if meta:
            vis = meta.get("visibility", "private")
            parts.append(f" [{vis}]")
            shared = meta.get("shared_with", {})
            if shared:
                parts.append(f" (shared with {len(shared)} node(s))")
        lines.append("".join(parts))

    return {"result": "\n".join(lines), "status": "completed"}


# ── ACL management actions ───────────────────────────────────────────

def _action_share(input_data: dict, vault_dir: Path,
                  caller_node_id: str, is_local: bool) -> dict:
    """Grant another node access to this vault.

    Required: node_id (target node), permission (read|write).
    Only the vault owner or a local caller can share.
    """
    target_node = input_data.get("node_id", "").strip()
    permission = input_data.get("permission", "read").strip().lower()

    if not target_node:
        return _error("node_id is required (the node to share with)")
    if permission not in ("read", "write"):
        return _error("permission must be 'read' or 'write'")

    meta = _load_vault_meta(vault_dir)
    if not meta and is_local:
        # Local vault with no .vault.json yet — create one owned by us
        meta = {
            "owner": _get_own_node_id(),
            "created": datetime.now(timezone.utc).isoformat(),
            "visibility": "private",
            "shared_with": {},
            "quota_bytes": VAULT_QUOTA_BYTES,
            "quota_docs": VAULT_QUOTA_DOCS,
            "used_bytes": 0,
        }
    elif not meta:
        return _error("Vault has no metadata — cannot share")

    # Only owner or local caller can share
    if not is_local and meta.get("owner") != caller_node_id:
        return _error("Only the vault owner can share access")

    shared = meta.get("shared_with", {})
    shared[target_node] = permission
    meta["shared_with"] = shared
    _save_vault_meta(vault_dir, meta)

    vault_label = vault_dir.name
    log.info("Shared vault '%s' with node %s… (%s)",
             vault_label, target_node[:16], permission)

    # Notify the invited node via knarr-mail (fire-and-forget)
    own_id = _get_own_node_id()
    _send_knarr_mail(
        to_node=target_node,
        content=json.dumps({
            "type": "vault_share",
            "message": (
                f"You've been granted {permission} access to vault "
                f"'{vault_label}' on node {own_id[:16]}…"
            ),
            "vault_name": vault_label,
            "permission": permission,
            "from_node": own_id,
        }),
    )

    return {
        "result": f"Granted {permission} access to node {target_node[:16]}… on vault '{vault_label}'",
        "vault": vault_label, "status": "completed",
    }


def _action_revoke(input_data: dict, vault_dir: Path,
                   caller_node_id: str, is_local: bool) -> dict:
    """Revoke a node's access to this vault.

    Required: node_id (the node to revoke).
    Only the vault owner or a local caller can revoke.
    """
    target_node = input_data.get("node_id", "").strip()
    if not target_node:
        return _error("node_id is required (the node to revoke)")

    meta = _load_vault_meta(vault_dir)
    if not meta:
        return _error("Vault has no metadata — nothing to revoke")

    if not is_local and meta.get("owner") != caller_node_id:
        return _error("Only the vault owner can revoke access")

    shared = meta.get("shared_with", {})
    if target_node not in shared:
        return _error(f"Node {target_node[:16]}… does not have access to this vault")

    del shared[target_node]
    meta["shared_with"] = shared
    _save_vault_meta(vault_dir, meta)

    vault_label = vault_dir.name
    log.info("Revoked access for node %s… on vault '%s'",
             target_node[:16], vault_label)

    return {
        "result": f"Revoked access for node {target_node[:16]}… on vault '{vault_label}'",
        "vault": vault_label, "status": "completed",
    }


def _action_set_visibility(input_data: dict, vault_dir: Path,
                           caller_node_id: str, is_local: bool) -> dict:
    """Change a vault's visibility mode.

    Required: visibility (private|public_read|public_write).
    Only the vault owner or a local caller can change visibility.
    """
    visibility = input_data.get("visibility", "").strip().lower()
    if visibility not in ("private", "public_read", "public_write"):
        return _error("visibility must be 'private', 'public_read', or 'public_write'")

    meta = _load_vault_meta(vault_dir)
    if not meta and is_local:
        meta = {
            "owner": _get_own_node_id(),
            "created": datetime.now(timezone.utc).isoformat(),
            "visibility": visibility,
            "shared_with": {},
            "quota_bytes": VAULT_QUOTA_BYTES,
            "quota_docs": VAULT_QUOTA_DOCS,
            "used_bytes": 0,
        }
    elif not meta:
        return _error("Vault has no metadata — cannot set visibility")
    else:
        if not is_local and meta.get("owner") != caller_node_id:
            return _error("Only the vault owner can change visibility")
        meta["visibility"] = visibility

    _save_vault_meta(vault_dir, meta)

    vault_label = vault_dir.name
    log.info("Set visibility of vault '%s' to '%s'", vault_label, visibility)

    return {
        "result": f"Vault '{vault_label}' visibility set to '{visibility}'",
        "vault": vault_label, "status": "completed",
    }


def _action_vault_info(vault_dir: Path, caller_node_id: str,
                       is_local: bool) -> dict:
    """Show vault metadata: owner, visibility, shared nodes, quota."""
    meta = _load_vault_meta(vault_dir)
    vault_label = vault_dir.name

    if not meta:
        # Local vault with no .vault.json
        if is_local:
            file_count = sum(1 for _ in vault_dir.rglob("*.md")
                             if not _.name.startswith(".") and not _.name.startswith("_"))
            return {
                "result": (
                    f"## Vault: {vault_label}\n"
                    f"Type: local (no ACL metadata)\n"
                    f"Files: {file_count}\n"
                    f"Git: {'yes' if (vault_dir / '.git').is_dir() else 'no'}"
                ),
                "vault": vault_label, "status": "completed",
            }
        return _error("No vault metadata available")

    # Has .vault.json
    owner = meta.get("owner", "unknown")
    vis = meta.get("visibility", "private")
    shared = meta.get("shared_with", {})
    quota = meta.get("quota_bytes", VAULT_QUOTA_BYTES)
    used = meta.get("used_bytes", 0)
    created = meta.get("created", "unknown")

    file_count = sum(1 for _ in vault_dir.rglob("*.md")
                     if not _.name.startswith(".") and not _.name.startswith("_"))

    lines = [
        f"## Vault: {vault_label}",
        f"Owner: {owner[:16]}…",
        f"Created: {created}",
        f"Visibility: {vis}",
        f"Files: {file_count}",
        f"Storage: {used:,} / {quota:,} bytes ({used * 100 // max(quota, 1)}%)",
        f"Git: {'yes' if (vault_dir / '.git').is_dir() else 'no'}",
    ]

    if shared:
        lines.append(f"\n### Shared With ({len(shared)})")
        for node_id, perm in shared.items():
            lines.append(f"- {node_id[:16]}… — {perm}")

    return {
        "result": "\n".join(lines),
        "vault": vault_label, "status": "completed",
    }


def _action_upload(path: str, content: str, vault_dir: Path,
                   description: str = "", url: str = "") -> dict:
    """Upload a binary file to the vault.

    Accepts either base64-encoded content or a URL to fetch. A sidecar
    metadata .md file is auto-created alongside the binary.
    """
    target = _safe_path_binary(path, vault_dir)
    if not target:
        return _error(
            f"Invalid path or unsupported file type: {path}. "
            f"Allowed extensions: {', '.join(sorted(_ALLOWED_BINARY_EXT))}"
        )

    # Get file bytes from base64 or URL
    if url:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "knarr-vault/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                file_bytes = resp.read(VAULT_MAX_UPLOAD_BYTES + 1)
        except Exception as e:
            return _error(f"Failed to fetch URL: {e}")
    elif content:
        try:
            file_bytes = base64.b64decode(content)
        except Exception:
            return _error("Invalid base64 content")
    else:
        return _error("Provide either 'content' (base64) or 'url' to upload")

    if len(file_bytes) > VAULT_MAX_UPLOAD_BYTES:
        return _error(
            f"File too large: {len(file_bytes):,} bytes "
            f"(max: {VAULT_MAX_UPLOAD_BYTES:,} bytes)"
        )

    # Write binary file
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(file_bytes)

    rel = str(target.relative_to(vault_dir))
    mime = mimetypes.guess_type(target.name)[0] or "application/octet-stream"

    # Create sidecar metadata file (asset_name.pdf → asset_name.pdf.md)
    sidecar = target.parent / (target.name + ".md")
    meta = {
        "type": "asset",
        "filename": target.name,
        "mime_type": mime,
        "size_bytes": len(file_bytes),
        "created": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }
    if url:
        meta["source_url"] = url
    body = description.strip() if description else f"Binary asset: {target.name}"
    sidecar_content = _serialize_frontmatter(meta) + "\n" + body + "\n"
    sidecar.write_text(sidecar_content, encoding="utf-8")

    sidecar_rel = str(sidecar.relative_to(vault_dir))
    _git_commit(f"vault: upload {rel}", vault_dir, [str(target), str(sidecar)])

    log.info("Uploaded %s in vault '%s' (%d bytes)", rel, vault_dir.name, len(file_bytes))

    return {
        "result": f"Uploaded: {rel} ({len(file_bytes):,} bytes, {mime}) [vault: {vault_dir.name}]",
        "path": rel, "vault": vault_dir.name, "status": "completed",
        "size_bytes": len(file_bytes), "mime_type": mime,
        "sidecar": sidecar_rel,
    }


def _action_download(path: str, vault_dir: Path) -> dict:
    """Download a binary file from the vault. Returns base64-encoded content."""
    target = _safe_path_binary(path, vault_dir)
    if not target:
        return _error(f"Invalid path or unsupported file type: {path}")
    if not target.exists():
        return _error(f"File not found: {path}")

    file_bytes = target.read_bytes()
    rel = str(target.relative_to(vault_dir))
    mime = mimetypes.guess_type(target.name)[0] or "application/octet-stream"

    # Read sidecar metadata if it exists
    sidecar = target.parent / (target.name + ".md")
    sidecar_meta = {}
    sidecar_desc = ""
    if sidecar.exists():
        sidecar_text = sidecar.read_text(encoding="utf-8")
        sidecar_meta, sidecar_desc = _parse_frontmatter(sidecar_text)

    encoded = base64.b64encode(file_bytes).decode("ascii")

    return {
        "result": f"Downloaded: {rel} ({len(file_bytes):,} bytes, {mime})",
        "path": rel, "vault": vault_dir.name, "status": "completed",
        "content_base64": encoded,
        "mime_type": mime, "size_bytes": len(file_bytes),
        "metadata": sidecar_meta, "description": sidecar_desc.strip(),
    }


def _action_delete(path: str, vault_dir: Path) -> dict:
    """Delete a file from the vault. For binary files, also removes the sidecar."""
    # Try as markdown first, then as binary
    target = _safe_path(path, vault_dir)
    is_binary = False
    if not target or not target.exists():
        target = _safe_path_binary(path, vault_dir)
        is_binary = True if target else False
    if not target:
        return _error(f"Invalid path: {path}")
    if not target.exists():
        return _error(f"File not found: {path}")

    rel = str(target.relative_to(vault_dir))
    target.unlink()
    log.info("Deleted %s from vault '%s'", rel, vault_dir.name)

    # If binary, also remove sidecar metadata file
    if is_binary:
        sidecar = target.parent / (target.name + ".md")
        if sidecar.exists():
            sidecar.unlink()
            log.info("Deleted sidecar %s", sidecar.name)
        _git_commit(f"vault: delete {rel}", vault_dir)
    else:
        _git_commit(f"vault: delete {rel}", vault_dir)
        _zvec_remove(vault_dir, doc_id=rel)

    return {
        "result": f"Deleted: {rel} [vault: {vault_dir.name}]",
        "path": rel, "vault": vault_dir.name, "status": "completed",
    }


def _action_query(filter_expr: str, vault_dir: Path,
                  sort_expr: str = "", limit: int = 0) -> dict:
    """Query all vault files by frontmatter fields with optional sorting.

    Examples:
      filter="type=lead,status=outreach"              — all outreach leads
      filter="value>1000", sort="value:desc"           — high-value, sorted
      filter="type=experiment", sort="updated:desc", limit=5  — last 5 experiments
    """
    return _action_list("", filter_expr, vault_dir, sort_expr, limit)


def _action_stats(vault_dir: Path) -> dict:
    """Dashboard view: counts by type, status, and recent activity.

    Returns a compact summary the agent can use to orient itself quickly.
    """
    type_counts: dict[str, int] = defaultdict(int)
    status_counts: dict[str, int] = defaultdict(int)
    type_status: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    total = 0
    recently_updated: list[tuple[str, str, dict]] = []  # (date, path, meta)
    total_value = 0.0
    value_count = 0

    for md_file in sorted(vault_dir.rglob("*.md")):
        if md_file.name.startswith(".") or md_file.name.startswith("_"):
            continue
        try:
            text = md_file.read_text(encoding="utf-8")
        except Exception:
            continue

        meta, _ = _parse_frontmatter(text)
        rel = str(md_file.relative_to(vault_dir))
        total += 1

        ftype = str(meta.get("type", "untyped"))
        fstatus = str(meta.get("status", "unknown"))
        type_counts[ftype] += 1
        status_counts[fstatus] += 1
        type_status[ftype][fstatus] += 1

        # Track value
        val = meta.get("value")
        if isinstance(val, (int, float)):
            total_value += val
            value_count += 1

        # Track recent updates
        updated = str(meta.get("updated", ""))
        if updated:
            recently_updated.append((updated, rel, meta))

    # Sort recent by date desc
    recently_updated.sort(key=lambda x: x[0], reverse=True)
    recent_5 = recently_updated[:5]

    # Build output
    lines = [f"## Vault Stats: '{vault_dir.name}'", f"**{total} files total**\n"]

    # By type with status breakdown
    if type_counts:
        lines.append("### By Type")
        for t, count in sorted(type_counts.items(), key=lambda x: -x[1]):
            status_breakdown = type_status[t]
            sb = ", ".join(f"{s}={c}" for s, c in sorted(status_breakdown.items()))
            lines.append(f"- **{t}**: {count} ({sb})")
        lines.append("")

    # Value summary
    if value_count:
        lines.append(f"### Pipeline Value")
        lines.append(f"- Total: {total_value:,.0f} across {value_count} valued files")
        lines.append(f"- Average: {total_value / value_count:,.0f}")
        lines.append("")

    # Recent activity
    if recent_5:
        lines.append("### Recently Updated")
        for date, path, meta in recent_5:
            label = meta.get("company") or meta.get("topic") or meta.get("type", "")
            lines.append(f"- {date} — **{path}** ({label})")
        lines.append("")

    # Directory breakdown
    dir_counts: dict[str, int] = defaultdict(int)
    for md_file in vault_dir.rglob("*.md"):
        if md_file.name.startswith(".") or md_file.name.startswith("_"):
            continue
        parent = md_file.parent.relative_to(vault_dir)
        dir_counts[str(parent)] += 1
    if dir_counts:
        lines.append("### By Directory")
        for d, c in sorted(dir_counts.items()):
            lines.append(f"- /{d}: {c} file(s)")

    return {
        "result": "\n".join(lines),
        "vault": vault_dir.name,
        "status": "completed",
    }


def _action_links(path: str, vault_dir: Path) -> dict:
    """Wiki-link graph: outgoing [[links]] from a file and backlinks to it.

    Supports Obsidian-style [[Target]] and [[Target|Display Text]] links.
    Returns both directions so the agent can traverse the knowledge graph.
    """
    target = _safe_path(path, vault_dir)
    if not target:
        return _error(f"Invalid path: {path}")
    if not target.exists():
        return _error(f"File not found: {path}")

    text = target.read_text(encoding="utf-8")
    meta, body = _parse_frontmatter(text)
    rel = str(target.relative_to(vault_dir))

    # 1. Outgoing links from this file
    outgoing = _extract_wikilinks(body)
    # Also check frontmatter text for links
    outgoing += [l for l in _extract_wikilinks(text) if l not in outgoing]

    # 2. Backlinks: scan all files for links pointing to this file
    # Match by filename (without .md) or full relative path
    target_stem = target.stem  # e.g. "burkhardt-ag"
    target_rel_no_ext = rel.rsplit(".md", 1)[0]  # e.g. "leads/burkhardt-ag"
    backlinks = []

    for md_file in vault_dir.rglob("*.md"):
        if md_file == target:
            continue
        if md_file.name.startswith(".") or md_file.name.startswith("_"):
            continue
        try:
            other_text = md_file.read_text(encoding="utf-8")
        except Exception:
            continue

        other_links = _extract_wikilinks(other_text)
        for link in other_links:
            link_lower = link.lower().strip()
            if link_lower == target_stem.lower() or \
               link_lower == target_rel_no_ext.lower() or \
               link_lower.endswith("/" + target_stem.lower()):
                other_rel = str(md_file.relative_to(vault_dir))
                other_meta, _ = _parse_frontmatter(other_text)
                label = other_meta.get("company") or other_meta.get("type", "")
                backlinks.append((other_rel, label))
                break

    # Build output
    lines = [f"## Links: {rel} [vault: {vault_dir.name}]"]

    if outgoing:
        lines.append(f"\n### Outgoing ({len(outgoing)})")
        for link in outgoing:
            link_path = _safe_path(link, vault_dir)
            exists = "✓" if link_path and link_path.exists() else "✗"
            lines.append(f"- [[{link}]] {exists}")
            # 2nd-degree: show what this linked doc links to
            if link_path and link_path.exists():
                try:
                    linked_body = link_path.read_text(encoding="utf-8")
                    second_links = _extract_wikilinks(linked_body)
                    # Filter out self-references back to the original doc
                    second_links = [sl for sl in second_links
                                    if sl.lower().strip() != target.stem.lower()]
                    if second_links:
                        lines.append(f"  → also links to: {', '.join(f'[[{s}]]' for s in second_links[:8])}")
                except Exception:
                    pass
    else:
        lines.append("\n### Outgoing: none")

    if backlinks:
        lines.append(f"\n### Backlinks ({len(backlinks)})")
        for bl_path, bl_label in backlinks:
            lines.append(f"- **{bl_path}** ({bl_label})")
    else:
        lines.append("\n### Backlinks: none")

    return {
        "result": "\n".join(lines),
        "path": rel, "vault": vault_dir.name, "status": "completed",
    }


def _action_update_meta(path: str, content: str, vault_dir: Path) -> dict:
    """Patch specific frontmatter fields without touching the body.

    content is a comma-separated list of key=value assignments:
      "status=outreach,value=10000,tags=[ai,zurich]"

    Only the specified fields are changed. All other frontmatter and the
    entire body are preserved exactly as-is. `updated` is bumped automatically.
    """
    target = _safe_path(path, vault_dir)
    if not target:
        return _error(f"Invalid path: {path}")
    if not target.exists():
        return _error(f"File not found: {path}")
    if not content.strip():
        return _error("No fields to update. Pass content='status=outreach,value=10000'")

    with _FileLock(target):
        text = target.read_text(encoding="utf-8")
        meta, body = _parse_frontmatter(text)

        if not meta:
            return _error(f"File {path} has no frontmatter to update")

        # Parse the update assignments
        changes = {}
        for part in content.split(","):
            part = part.strip()
            if "=" not in part:
                continue
            key, val = part.split("=", 1)
            key = key.strip()
            val = val.strip()

            # Strip quotes
            if (val.startswith('"') and val.endswith('"')) or \
               (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]

            # Parse typed values (same as frontmatter parser)
            if val.startswith("[") and val.endswith("]"):
                items = [v.strip().strip("'\"") for v in val[1:-1].split(",") if v.strip()]
                changes[key] = items
            elif val.replace(".", "", 1).replace("-", "", 1).isdigit():
                changes[key] = float(val) if "." in val else int(val)
            elif val.lower() in ("true", "yes"):
                changes[key] = True
            elif val.lower() in ("false", "no"):
                changes[key] = False
            else:
                changes[key] = val

        if not changes:
            return _error("No valid key=value pairs found in content")

        # Apply changes
        old_vals = {}
        for k, v in changes.items():
            old_vals[k] = meta.get(k)
            meta[k] = v

        meta["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Rewrite file with patched frontmatter + original body
        final = _serialize_frontmatter(meta) + "\n" + body.lstrip("\n")
        target.write_text(final, encoding="utf-8")

    rel = str(target.relative_to(vault_dir))
    changed_summary = ", ".join(f"{k}: {old_vals[k]} → {changes[k]}" for k in changes)
    log.info("Updated meta on %s: %s", rel, changed_summary)
    _git_commit(f"vault: update_meta {rel} ({', '.join(changes.keys())})",
                vault_dir, [str(target)])

    _check_notify(meta, rel, vault_dir.name, "update_meta")

    return {
        "result": f"Updated {len(changes)} field(s) on {rel}: {changed_summary} [vault: {vault_dir.name}]",
        "path": rel, "vault": vault_dir.name, "status": "completed",
    }


def _action_history(path: str, vault_dir: Path, limit: int = 10) -> dict:
    """Show git history for a file or the entire vault.

    If path is provided, shows commits affecting that file.
    If path is empty, shows recent vault-wide commits.
    """
    if not _git_available(vault_dir):
        return _error("Git not available for this vault — no history tracking")

    cmd = ["git", "log", f"--max-count={limit}",
           "--format=%h|%ai|%s"]  # hash|date|message

    if path.strip():
        target = _safe_path(path, vault_dir)
        if not target:
            return _error(f"Invalid path: {path}")
        # Use -- to separate path from options
        rel = str(target.relative_to(vault_dir))
        cmd += ["--", rel]
        title = f"## History: {rel}"
    else:
        title = f"## History: vault '{vault_dir.name}'"

    try:
        result = subprocess.run(
            cmd, cwd=vault_dir, capture_output=True, text=True, timeout=10,
        )
        raw = result.stdout.strip()
    except Exception as e:
        return _error(f"Failed to read git history: {e}")

    if not raw:
        return {"result": f"{title}\nNo history found.",
                "vault": vault_dir.name, "status": "completed"}

    lines = [title]
    for entry in raw.splitlines():
        parts = entry.split("|", 2)
        if len(parts) == 3:
            commit_hash, date, message = parts
            # Trim date to just date+time (no timezone)
            date_short = date[:16]
            lines.append(f"- `{commit_hash}` {date_short} — {message}")
        else:
            lines.append(f"- {entry}")

    lines.append(f"\n{len(raw.splitlines())} commit(s) shown (limit={limit})")

    return {
        "result": "\n".join(lines),
        "vault": vault_dir.name, "status": "completed",
    }


def _action_move(path: str, content: str, vault_dir: Path) -> dict:
    """Move/rename a file within the vault.

    path    = source file (e.g. "notes/acme-research")
    content = destination path (e.g. "leads/acme")

    Preserves frontmatter, bumps `updated`. Uses git mv when available
    so the commit history stays linked.
    """
    source = _safe_path(path, vault_dir)
    if not source:
        return _error(f"Invalid source path: {path}")
    if not source.exists():
        return _error(f"Source not found: {path}")

    dest_path = content.strip()
    if not dest_path:
        return _error("No destination path. Pass content='leads/new-name'")

    dest = _safe_path(dest_path, vault_dir)
    if not dest:
        return _error(f"Invalid destination path: {dest_path}")

    if dest.exists():
        return _error(f"Destination already exists: {dest_path}")

    with _FileLock(source):
        # Read and update metadata
        text = source.read_text(encoding="utf-8")
        meta, body = _parse_frontmatter(text)
        if meta:
            meta["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            final = _serialize_frontmatter(meta) + "\n" + body.lstrip("\n")
        else:
            final = text

        # Create destination directory
        dest.parent.mkdir(parents=True, exist_ok=True)

        # Try git mv first (preserves history linkage)
        source_rel = str(source.relative_to(vault_dir))
        dest_rel = str(dest.relative_to(vault_dir))

        if _git_available(vault_dir):
            try:
                result = subprocess.run(
                    ["git", "mv", source_rel, dest_rel],
                    cwd=vault_dir, capture_output=True, text=True, env=_git_env(),
                )
                if result.returncode == 0:
                    # Write updated content to destination
                    dest.write_text(final, encoding="utf-8")
                    _git_commit(f"vault: move {source_rel} → {dest_rel}",
                                vault_dir, [dest_rel])
                    log.info("Moved %s → %s (git mv)", source_rel, dest_rel)
                    return {
                        "result": f"Moved: {source_rel} → {dest_rel} [vault: {vault_dir.name}]",
                        "path": dest_rel, "vault": vault_dir.name, "status": "completed",
                    }
            except Exception:
                pass  # Fall through to manual move

        # Manual move fallback
        dest.write_text(final, encoding="utf-8")
        source.unlink()
        _git_commit(f"vault: move {source_rel} → {dest_rel}", vault_dir)

    log.info("Moved %s → %s (manual)", source_rel, dest_rel)
    return {
        "result": f"Moved: {source_rel} → {dest_rel} [vault: {vault_dir.name}]",
        "path": dest_rel, "vault": vault_dir.name, "status": "completed",
    }


def _action_export(filter_expr: str, vault_dir: Path,
                   sort_expr: str = "", limit: int = 0) -> dict:
    """Export query results as CSV.

    Uses the same filter/sort/limit as query, but returns comma-separated
    values instead of markdown. All frontmatter fields become columns.
    The first row is the header. Path is always the first column.

    Great for dumping leads into a spreadsheet, generating reports, or
    feeding data to other tools.
    """
    filters = _parse_filters(filter_expr)
    sort = _parse_sort(sort_expr)
    files = _collect_files(vault_dir, vault_dir, filters, sort, limit)

    if not files:
        msg = "No files to export"
        if filter_expr:
            msg += f" matching {filter_expr}"
        return {"result": msg, "vault": vault_dir.name, "status": "completed"}

    # Collect all unique frontmatter keys across all files
    all_keys: list[str] = []
    for _, meta, _ in files:
        for k in meta:
            if k not in all_keys:
                all_keys.append(k)

    # Build CSV
    def _csv_escape(val) -> str:
        s = str(val) if val is not None else ""
        if isinstance(val, list):
            s = "; ".join(str(v) for v in val)
        # Escape quotes and wrap if contains comma/quote/newline
        if "," in s or '"' in s or "\n" in s:
            s = '"' + s.replace('"', '""') + '"'
        return s

    header = ["path"] + all_keys
    rows = [",".join(header)]

    for rel, meta, body in files:
        row = [_csv_escape(rel)]
        for k in all_keys:
            row.append(_csv_escape(meta.get(k, "")))
        rows.append(",".join(row))

    csv_text = "\n".join(rows)
    count = len(files)

    return {
        "result": f"## CSV Export ({count} rows, {len(all_keys)} fields) [vault: {vault_dir.name}]\n\n{csv_text}",
        "vault": vault_dir.name,
        "status": "completed",
    }


# ── Self-documentation ───────────────────────────────────────────────

_HELP_TEXT = """\
# Knowledge Vault — Multi-Tenant Agent Knowledge Base

A file-system-backed knowledge base with per-caller isolation. Every document \
is a Markdown file with YAML frontmatter. Git-versioned, multi-vault, \
Obsidian-compatible. Network callers get auto-scoped private vaults.

## Actions

| Action          | Purpose                          | Required fields            | Optional fields           |
|-----------------|----------------------------------|----------------------------|---------------------------|
| help            | Show this documentation          | (none)                     |                           |
| write           | Create or overwrite a file       | path, content              | vault_name                |
| append          | Add to an existing file          | path, content              | vault_name                |
| update_meta     | Patch frontmatter fields only    | path, content              | vault_name                |
| read            | Read a file                      | path                       | vault_name                |
| list            | List files in a directory        | (none)                     | path, filter, sort, limit |
| search          | Search (semantic + text)         | query                      | vault_name                |
| semantic_search | Semantic similarity search       | query                      | vault_name                |
| search_all      | Search across all vaults         | query                      |                           |
| query           | Filter by frontmatter fields     | filter                     | sort, limit, vault_name   |
| stats           | Dashboard / summary counts       | (none)                     | vault_name                |
| links           | Wiki-link graph for a file       | path                       | vault_name                |
| history         | Git changelog for file/vault     | (none)                     | path, limit, vault_name   |
| move            | Rename/relocate a file           | path, content (=dest)      | vault_name                |
| export          | Export query results as CSV      | (none)                     | filter, sort, limit       |
| upload          | Upload a binary file             | path, content OR url       | description, vault_name   |
| download        | Download a binary file           | path                       | vault_name                |
| delete          | Remove file (binary + sidecar)   | path                       | vault_name                |
| list_vaults     | List all accessible vaults       | (none)                     |                           |
| vault_info      | Show vault ACL and quota         | (none)                     | vault_name                |
| share           | Grant node access to vault       | node_id                    | vault_name, permission    |
| revoke          | Revoke node access               | node_id                    | vault_name                |
| set_visibility  | Change vault visibility          | visibility                 | vault_name                |

## Multi-Tenant Access

Network callers are automatically scoped to isolated vaults based on their \
cryptographic node identity. Local callers (the bot itself) have full access \
to all vaults.

Visibility modes:
- **private** (default) — only owner + explicitly shared nodes
- **public_read** — any node can read, only owner/shared can write
- **public_write** — any node can read and write (wiki-style)

Sharing:
  action=share, vault_name=research, node_id=abc123..., permission=write
  action=revoke, vault_name=research, node_id=abc123...
  action=set_visibility, vault_name=wiki, visibility=public_write

Quotas: Network vaults have storage limits (default 100MB). Use vault_info \
to check usage.

## Binary Assets

Upload binary files (images, PDFs, CSVs, etc.) with action=upload:
  action=upload, path=assets/report.pdf, content=<base64>, description="Q1 report"
  action=upload, path=assets/photo.png, url=https://example.com/photo.png

A sidecar metadata file (assets/report.pdf.md) is auto-created with type=asset \
frontmatter, making it searchable and queryable like any vault document.

Download with action=download, path=assets/report.pdf (returns base64).

Allowed file types: png, jpg, jpeg, gif, webp, svg, pdf, csv, json, xml, txt, \
xlsx, xls, zip, tar, gz, mp3, wav, ogg, mp4, webm. Max upload: 10MB per file.

## File Format

Always use YAML frontmatter for structured metadata:

```
---
type: lead
status: outreach
company: Acme Corp
contact: Jane Smith
value: 5000
tags: [ai-consulting, zurich]
created: 2026-02-10
updated: 2026-02-10
---

# Acme Corp

## Research Notes
Found via LinkedIn. 50-person company in industrial automation.
Met [[jane-smith]] at conference.

## Outreach Log
**2026-02-10** — Sent intro email.
```

## Recommended Directory Structure

- leads/       — one file per lead/prospect
- experiments/ — numbered experiments (hypothesis -> approach -> results)
- reports/     — research reports, intelligence briefings
- projects/    — project plans, deliverables
- contacts/    — people and relationships
- notes/       — general working notes

## Filtering

filter="type=lead,status=outreach"     — exact match (AND logic)
filter="value>5000"                    — numeric greater-than
filter="value<1000"                    — numeric less-than
filter="tags=zurich"                   — matches if value is in list

## Sorting

sort="value:desc"      — highest value first
sort="updated:desc"    — most recently updated first
sort="company:asc"     — alphabetical

Combine with limit=10 for "top N" queries.

## Wiki-Links

Use [[filename]] (without .md) in your markdown body to link between files. \
The `links` action shows outgoing links and backlinks, turning the vault into \
a knowledge graph. Supports [[Target|Display Text]] syntax.

## Multi-Vault

By default everything goes to the "default" vault. Pass vault_name="sales" to \
route to an isolated vault. Each vault has its own directory and git history.

## Tips

- Call action=stats first to orient yourself — see what's in the vault.
- Use action=list_vaults to see all vaults you have access to.
- Use action=vault_info to check quota and sharing settings.
- Use action=update_meta to change status, value, tags — one call, no rewrite.
- Use action=append to add entries (outreach logs, results) instead of rewriting.
- Use action=query with sort and limit for "top N" and "most recent" views.
- Use action=export to dump data as CSV for spreadsheets or other tools.
- Use action=search for hybrid search (semantic when available, text fallback).
- Use action=semantic_search for dedicated vector similarity search.
- Use action=search_all to find something across all accessible vaults.
- Use action=history to see what changed recently.
- Use [[wiki-links]] to connect related files (leads <-> contacts <-> projects).
"""


def _action_help() -> dict:
    """Return comprehensive usage documentation for the vault skill."""
    return {
        "result": _HELP_TEXT,
        "status": "completed",
    }


# ── Skill entry point ────────────────────────────────────────────────

# Actions that operate within a single vault (receive vault_dir)
_VAULT_ACTIONS = {
    "write":       lambda d, v: _action_write(d.get("path", ""), d.get("content", ""), v),
    "append":      lambda d, v: _action_append(d.get("path", ""), d.get("content", ""), v),
    "read":        lambda d, v: _action_read(d.get("path", ""), v),
    "list":        lambda d, v: _action_list(
        d.get("path", ""), d.get("filter", ""), v,
        d.get("sort", ""), _int_or(d.get("limit", ""), 0)),
    "search":      lambda d, v: _action_search(d.get("query", "") or d.get("content", ""), v),
    "semantic_search": lambda d, v: _action_semantic_search(d.get("query", "") or d.get("content", ""), v),
    "query":       lambda d, v: _action_query(
        d.get("filter", "") or d.get("query", ""), v,
        d.get("sort", ""), _int_or(d.get("limit", ""), 0)),
    "stats":       lambda d, v: _action_stats(v),
    "links":       lambda d, v: _action_links(d.get("path", ""), v),
    "update_meta": lambda d, v: _action_update_meta(d.get("path", ""), d.get("content", ""), v),
    "history":     lambda d, v: _action_history(
        d.get("path", ""), v, _int_or(d.get("limit", ""), 10)),
    "move":        lambda d, v: _action_move(d.get("path", ""), d.get("content", ""), v),
    "export":      lambda d, v: _action_export(
        d.get("filter", "") or d.get("query", ""), v,
        d.get("sort", ""), _int_or(d.get("limit", ""), 0)),
    "delete":      lambda d, v: _action_delete(d.get("path", ""), v),
    "upload":      lambda d, v: _action_upload(
        d.get("path", ""), d.get("content", ""), v,
        d.get("description", ""), d.get("url", "")),
    "download":    lambda d, v: _action_download(d.get("path", ""), v),
}

# ACL-management actions (owner or local only, receive vault_dir + caller info)
_ACL_ACTIONS = {"share", "revoke", "set_visibility", "vault_info"}

# Actions that span all vaults or need no vault_dir
_GLOBAL_ACTIONS = {"help", "search_all", "list_vaults"}

ALL_ACTIONS = set(_VAULT_ACTIONS.keys()) | _ACL_ACTIONS | _GLOBAL_ACTIONS


def _int_or(val, default: int) -> int:
    """Safely convert to int, returning default on failure."""
    if isinstance(val, int):
        return val
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


async def handle(input_data: dict, ctx=None) -> dict:
    """Knowledge Vault — multi-tenant agent knowledge base.

    Call action=help for full documentation including file format, filtering,
    sorting, wiki-links, multi-vault, and sharing support.

    Actions: help, write, append, update_meta, read, list, search, semantic_search,
             search_all, query, stats, links, history, move, export, delete,
             upload, download, share, revoke, set_visibility, vault_info, list_vaults.
    """
    action = input_data.get("action", "").strip().lower()
    caller = input_data.get("_caller_node_id", "")

    if not action:
        return _action_help()

    if action not in ALL_ACTIONS:
        return _error(f"Unknown action: {action}. Valid: {', '.join(sorted(ALL_ACTIONS))}")

    # ── Global actions (no single vault) ──
    if action == "help":
        return _action_help()
    if action == "list_vaults":
        try:
            return _action_list_vaults(input_data, caller)
        except Exception as e:
            log.exception("Vault action '%s' failed", action)
            return _error(f"{type(e).__name__}: {e}")
    if action == "search_all":
        try:
            return _action_search_all(
                d=input_data,
                caller_node_id=caller,
            )
        except Exception as e:
            log.exception("Vault action '%s' failed", action)
            return _error(f"{type(e).__name__}: {e}")

    # ── Resolve vault (all remaining actions are vault-scoped) ──
    vault_dir, is_local = _resolve_vault(input_data)
    vault_dir.mkdir(parents=True, exist_ok=True)
    _init_vault_git(vault_dir)

    # Auto-create .vault.json for foreign callers on first access
    if not is_local:
        _ensure_vault_meta(vault_dir, caller)

    # ── ACL-management actions ──
    if action in _ACL_ACTIONS:
        try:
            if action == "share":
                return _action_share(input_data, vault_dir, caller, is_local)
            elif action == "revoke":
                return _action_revoke(input_data, vault_dir, caller, is_local)
            elif action == "set_visibility":
                return _action_set_visibility(input_data, vault_dir, caller, is_local)
            elif action == "vault_info":
                return _action_vault_info(vault_dir, caller, is_local)
        except Exception as e:
            log.exception("Vault action '%s' failed", action)
            return _error(f"{type(e).__name__}: {e}")

    # ── ACL check for vault-scoped actions ──
    is_write = action in _WRITE_ACTIONS
    if not _check_access(vault_dir, caller, is_local, write=is_write):
        perm = "write" if is_write else "read"
        return _error(f"Access denied: you do not have {perm} permission on this vault")

    # ── Quota check for write actions by foreign callers ──
    if is_write and not is_local:
        over, msg = _check_quota(vault_dir)
        if over:
            return _error(msg)

    try:
        result = _VAULT_ACTIONS[action](input_data, vault_dir)
        # Update used_bytes after successful write by foreign caller
        if is_write and not is_local:
            _update_used_bytes(vault_dir)
        return result
    except TimeoutError as e:
        return _error(str(e))
    except Exception as e:
        log.exception("Vault action '%s' failed", action)
        return _error(f"{type(e).__name__}: {e}")


def _error(message: str) -> dict:
    return {
        "result": f"ERROR: {message}",
        "path": "", "status": "error", "error": message,
    }
