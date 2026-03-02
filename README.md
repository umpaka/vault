# knarr-vault

Private, file-system-backed long-term memory for KNARR agents.

Every document is a Markdown file with YAML frontmatter. Git-versioned, multi-vault, Obsidian-compatible. Agents write structured knowledge; humans open the folder and see a wiki.

---

## Architecture

There are three distinct vault patterns. Understanding them prevents security mistakes:

### 1 — Local private vault (default)
Installed on each agent's own VPS. `skill.toml` sets `visibility = "private"` — the skill is never advertised on the knarr DHT. Only the local agent can call it. No other node can discover or access it.

This is the correct setup for **personal agent memory**.

### 2 — Peer-to-peer shared vault
Agent A explicitly shares a sub-vault with Agent B by node ID (`action=share`). Data lives on A's VPS. B has the access level A granted (read or write). Nothing emerges automatically — sharing is always a deliberate act.

This is how **two agents collaborate** on a project without involving any third party.

### 3 — Network vault-as-a-service
The skill is exposed on the knarr network (`visibility = "network"` in skill.toml). Other nodes can call it. Foreign callers are automatically scoped to isolated `node-{prefix}/` namespaces — they cannot access each other's data or the host's private vaults.

This is how **umpaka runs a shared knowledge commons** at the network level.

---

## Security model

- **`_caller_node_id`** is injected by the knarr routing layer, derived from the caller's cryptographic public key. It cannot be spoofed.
- Foreign callers are always rooted under `node-{their_prefix}/` — path traversal to other namespaces is impossible.
- Local vaults (no `.vault.json`) are inaccessible to foreign callers even if the skill is network-exposed.
- **Set `KNARR_NODE_ID` in `.env`.** Without it the vault uses an API fallback. If that also fails, the vault fails-closed (foreign callers get scoped, not promoted to local).

---

## Installation

### On any knarr node

```bash
# Clone into your knarr skills directory
git clone https://github.com/umpaka/vault /opt/knarr-skills/vault

# Copy and configure .env
cp /opt/knarr-skills/vault/.env.example /opt/knarr-skills/vault/.env
# Edit .env — set KNARR_NODE_ID at minimum

# Register the skill in knarr.toml
# Add to [skills] section:
#   [[skills]]
#   path = "/opt/knarr-skills/vault"
```

---

## Vault data directory

By default vaults live in `/opt/knarr-vault/`:

```
/opt/knarr-vault/
├── default/          ← local bot's default vault
├── sales/            ← named vault (VAULT_CHANNEL_MAP or vault_name=)
└── node-{prefix}/    ← auto-created for each foreign network caller
    └── default/
```

The directory is Obsidian-compatible — open it as a vault and browse your agent's memory as a wiki.

---

## Actions

| Action           | Purpose                              |
|------------------|--------------------------------------|
| `write`          | Create or overwrite a document       |
| `append`         | Add to an existing document          |
| `update_meta`    | Patch frontmatter fields only        |
| `read`           | Read a document                      |
| `list`           | List documents in a directory        |
| `search`         | Search (semantic + text fallback)    |
| `search_all`     | Search across all accessible vaults  |
| `query`          | Filter by frontmatter fields         |
| `stats`          | Summary: counts, types, recent edits |
| `links`          | Wiki-link graph for a document       |
| `history`        | Git changelog                        |
| `move`           | Rename / relocate a document         |
| `export`         | Export query results as CSV          |
| `upload`         | Upload a binary file                 |
| `download`       | Download a binary file               |
| `delete`         | Remove a document                    |
| `share`          | Grant a node access to a vault       |
| `revoke`         | Revoke a node's access               |
| `set_visibility` | Change vault visibility mode         |
| `vault_info`     | Show ACL and quota                   |
| `list_vaults`    | List all accessible vaults           |
| `help`           | Full inline documentation            |

Call `action=help` for detailed field reference and examples.

---

## Document format

```markdown
---
type: lead
status: outreach
company: Acme Corp
value: 5000
tags: [ai, zurich]
created: 2026-03-02
updated: 2026-03-02
---

# Acme Corp

Notes here. Use [[wiki-links]] to connect documents.
```

---

## Sharing example

```
# Share a vault sub-folder with another agent (read-only)
action=share, vault_name=research, node_id=<their_node_id>, permission=read

# Make a vault publicly readable on the network
action=set_visibility, vault_name=wiki, visibility=public_read

# Revoke access
action=revoke, vault_name=research, node_id=<their_node_id>
```

---

## Optional: semantic search

Install `zvec` and set `GEMINI_API_KEY` to enable vector similarity search. Documents are indexed automatically on write.

```bash
pip install zvec
```

---

## License

AGPL-3.0 — see [LICENSE](LICENSE).
