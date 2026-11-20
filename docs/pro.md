# Crewmesh Pro

agent-mesh stays a local, MIT-licensed mesh. **Crewmesh** is the product name for the hosted
layer. This repository, the `mesh-*` crates, and the `agent-mesh` binary are not being renamed.

Pro is **$9/month or $90/year**. It adds:

- Cross-machine mesh. Agents on different machines exchange messages through a hosted relay.
- Alerts (phone or Slack) when an agent finishes, errors, or needs input. Not built yet.
- Cloud session history. Not built yet.
- A cost dashboard on top of the existing usage tracking. Not built yet.

A Team tier ($15/user/month, shared hub, audit log) and a founding-member price ($5/month for
life, first 100) are planned and not implemented.

Logging in does not change the local mesh. Pro features turn on only when the account's
entitlement says so. A user who never logs in makes no network call: the hub stats a credentials
file that is not there, and stops.

## Account commands

```bash
agent-mesh login                      # device code; prints a URL to approve
agent-mesh login --token "$TOKEN"     # non-interactive
agent-mesh login --api-base http://127.0.0.1:8787
agent-mesh logout
agent-mesh status                     # whoami is an alias
```

Credentials and the entitlement cache live in `$AGENT_MESH_CONFIG_DIR`, or
`$XDG_CONFIG_HOME/agent-mesh`, or `~/.config/agent-mesh` (the same directory as `agents.toml`).
The directory is mode `0700`. `credentials.json` and `entitlement.json` are mode `0600`.

`status` exits 0 when someone is logged in and 1 when they are not.

## How "is Pro enabled?" works

`mesh_pro::pro_enabled()` is the question the rest of the code asks.

1. No credentials file: return `false`. No HTTP client is built.
2. Cached answer still inside `valid_until`: return it. No HTTP.
3. Otherwise refresh `GET /v1/entitlement` with a short timeout
   (`AGENT_MESH_ENTITLEMENT_TIMEOUT_MS`, default 2000).
4. If that refresh fails and the last successful check was within the grace window
   (`AGENT_MESH_ENTITLEMENT_GRACE_SECONDS`, default 86400) and it said Pro, keep Pro.
   A revoked token (`401`) does not get that grace.
5. Otherwise Pro is off.

The server tells the client how long to cache via `cache_ttl_seconds` (default 900, from
`CREWMESH_ENTITLEMENT_TTL_SECONDS` on the relay).

The hub keeps a local flag, refreshed by a background task. `send_message` to a node on this
machine never waits on that task. Addressing `remote:<machine>:<node>` without Pro returns:

```text
cross-machine mesh is a Crewmesh Pro feature ($9/mo or $90/yr). Run `agent-mesh login`, then upgrade at https://crewmesh.dev/pro
```

With Pro, `list_nodes` also shows peers the relay knows about. Their id is
`remote:<machine_id>:<node_id>`, which `send_message` routes back through the relay. A reply uses
that same id, so it lands on the machine that sent it. The hub re-registers and polls about every
couple of seconds after login; you do not restart it by hand.

## Client environment

| Variable | Default | Role |
|---|---|---|
| `AGENT_MESH_API_BASE` | `https://api.crewmesh.dev` | Entitlement and relay API. Overrides the base saved at login. |
| `AGENT_MESH_UPGRADE_URL` | `https://crewmesh.dev/pro` | Link in the Pro-required message. |
| `AGENT_MESH_CONFIG_DIR` | `~/.config/agent-mesh` | Credentials and entitlement cache. |
| `AGENT_MESH_ENTITLEMENT_GRACE_SECONDS` | `86400` | How long a cached Pro answer survives an outage. |
| `AGENT_MESH_ENTITLEMENT_TIMEOUT_MS` | `2000` | Refresh timeout. Offline use does not wait longer than this. |
| `AGENT_MESH_HOME` | `~/.agent-mesh` | Hub socket, and the stable `machine-id` used by the relay. |

There is no hosted API at the default base yet. Point `AGENT_MESH_API_BASE` at `agent-mesh-relay`.

## Relay

`agent-mesh-relay` is a separate binary (`cargo run -p mesh-relay`). It is a small HTTP service.
One account's machines share a queue; another account cannot read or address those nodes. A node
that is not re-registered within 45 seconds drops off the peer list.

| Method | Path | Auth |
|---|---|---|
| `GET` | `/healthz` | no |
| `GET` | `/v1/meta` | no; reports `"billing": "stub"` or `"stripe"` |
| `POST` | `/v1/device/start` | no |
| `POST` | `/v1/device/token` | no; poll until approved |
| `POST` | `/v1/device/approve` | stub billing only |
| `GET` | `/v1/device/verify?code=` | no; HTML instructions |
| `GET` | `/v1/entitlement` | bearer |
| `POST` | `/v1/checkout/session` | device code or bearer |
| `GET` | `/v1/checkout/stub/{id}` | stub billing only |
| `POST` | `/v1/checkout/stub/{id}/complete` | stub billing only; marks the account Pro |
| `POST` | `/v1/stripe/webhook` | Stripe signature when a webhook secret is set |
| `POST` | `/v1/relay/register` | Pro bearer |
| `POST` | `/v1/relay/send` | Pro bearer |
| `POST` | `/v1/relay/poll` | Pro bearer |
| `GET` | `/v1/relay/peers?machine_id=` | Pro bearer |

Messages are JSON. The client crate is `mesh-pro`; the server crate is `mesh-relay`. Both share
the types in `mesh_pro::protocol`.

### Relay environment

| Variable | Default | Role |
|---|---|---|
| `CREWMESH_RELAY_BIND` | `127.0.0.1:8787` | Listen address. |
| `CREWMESH_PUBLIC_URL` | `http://127.0.0.1:<port>` | Base URL printed in device and stub-checkout links. |
| `CREWMESH_RELAY_DATA` | unset (memory only) | JSON file for accounts, device codes, and checkout sessions. Written mode `0600`. Holds bearer tokens. A restart with this unset logs everyone out. Presence and undelivered relay messages are not persisted. |
| `CREWMESH_UPGRADE_URL` | `https://crewmesh.dev/pro` | Included in `403` responses. |
| `CREWMESH_ENTITLEMENT_TTL_SECONDS` | `900` | `cache_ttl_seconds` on entitlement responses. |
| `CREWMESH_STRIPE_SECRET_KEY` | unset | When unset, billing is stubbed. When set, checkout calls Stripe. |
| `CREWMESH_STRIPE_WEBHOOK_SECRET` | required with the secret key | Verifies `Stripe-Signature`. |
| `CREWMESH_STRIPE_PRICE_MONTHLY` | required with the secret key | Price id for `interval: "month"`. |
| `CREWMESH_STRIPE_PRICE_YEARLY` | required with the secret key | Price id for `interval: "year"`. |
| `CREWMESH_STRIPE_SUCCESS_URL` | `{public}/billing/success` | Checkout success redirect. |
| `CREWMESH_STRIPE_CANCEL_URL` | `{public}/billing/cancel` | Checkout cancel redirect. |
| `CREWMESH_STRIPE_API_BASE` | `https://api.stripe.com` | Override for tests. Not needed in production. |
| `RUST_LOG` or `AGENT_MESH_LOG` | `info` | Relay log filter. |

Do not put any of these values in the repo.

## What is stubbed, and what is real

**Real**

- Device login, bearer tokens, and `GET /v1/entitlement`.
- The relay: register, route by id or name, poll, account isolation, presence expiry.
- Stripe Checkout Session creation when `CREWMESH_STRIPE_SECRET_KEY` is set. The relay POSTs
  `application/x-www-form-urlencoded` to `/v1/checkout/sessions` with `mode=subscription`,
  `client_reference_id`, `metadata[account_id]`, `metadata[device_code]`, and
  `subscription_data[metadata][account_id]`.
- Webhook signature verification (HMAC-SHA256 over `{timestamp}.{body}`, five-minute tolerance)
  when `CREWMESH_STRIPE_WEBHOOK_SECRET` is set. `checkout.session.completed` sets Pro and approves
  the device code. `customer.subscription.deleted` sets the plan back to free.

**Stub, on purpose, until a Stripe account exists**

- With the secret key unset, `POST /v1/checkout/session` returns `"stub": true` and a local URL.
  `POST /v1/checkout/stub/{id}/complete` marks the account Pro. No card is charged.
- `POST /v1/device/approve` with `{"user_code","plan":"pro"|"free"}` approves a login directly.
- `POST /v1/stripe/webhook` accepts an unsigned Stripe-shaped JSON body and logs that it did.
- Those three routes return **404** once the Stripe secret is set, with a message that says why.
  `GET /v1/meta` reports which mode is active.

Stub mode is not safe on a public network: anyone who can reach the process can approve a Pro
account. Run it on localhost, or set the Stripe variables before binding a public address.

## Suggested next steps

- Alerts when a node finishes, errors, or is waiting on input (phone, Slack).
- Cloud session history, opt-in, keyed by the same account.
- A cost dashboard over the usage the hub already records.
- Team tier: shared hub, seats, audit log.
- Founding-member price for the first 100 accounts.
- Persist relay queues if a multi-instance relay needs them. Today one process owns the queues.
