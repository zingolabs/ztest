# seed-cdn — runbook

**This is ztest's read path.** Every seed pull — developer, CI, `snapshot warm`, every pod —
fetches from this Worker over plain HTTPS with no credentials. There is no authenticated
alternative: the library carries no S3 client. Credentials exist only for
`ztest snapshot push` (see [Push credentials](#push-credentials)).

Deployed at **https://ztest-seeds.elicbarbieri.workers.dev** — the `base_uri` in every
manifest.

Why a Worker and not the bucket's public URL: `r2.dev` is rate-limited and bandwidth
throttled *by design*, and the throttle is **variable**. That URL is now **disabled**
(`wrangler r2 bucket dev-url disable ztest-archives`), so this Worker is the bucket's only
public read path and the `lfs/<64 hex>` key pattern below cannot be sidestepped. Measured on one object across a
day: 1.4 MB/s at its worst, 23.7 MB/s at its best, with three multi-hour pulls killed
mid-stream in between. The Worker is not reliably faster at any given instant — a
same-minute comparison put them within noise of each other — it is the endpoint with no
documented throttle to collapse. Predictability is the win, not peak throughput.

## Deploy — four commands

```sh
cd workers/seed-cdn
npx wrangler login                    # OAuth; pick the account owning ztest-archives
npx wrangler whoami                   # copy the Account ID
$EDITOR wrangler.jsonc                # paste it into "account_id"
npx wrangler deploy                   # prints https://ztest-seeds.<subdomain>.workers.dev
```

`account_id` is not optional here. A login that can reach more than one account otherwise
deploys to whichever it was created against, and the bucket lives in exactly one — pinned,
a wrong-account deploy fails instead of half-working.

**If `deploy` says "You need to register a workers.dev subdomain":** the account has one but
this script's workers.dev route is off. Interactively, `wrangler deploy` offers to fix it —
answer yes. Non-interactively it declines and errors, and the route is one API call:

```sh
A=<account-id>; T=$(sed -n 's/^oauth_token *= *"\(.*\)"/\1/p' ~/.config/.wrangler/config/default.toml)
curl -s -X POST -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"enabled":true,"previews_enabled":false}' \
  "https://api.cloudflare.com/client/v4/accounts/$A/workers/scripts/ztest-seeds/subdomain"
```

## Verify

ztest checks its own read path — there is no script here to run or keep in step:

```sh
ztest cluster check        # `snapshot bucket` row: reachable *and* honours Range
ztest snapshot verify      # every declared blob, then the endpoint: seed keys only, writes refused
```

The range half is the load-bearing one. Seeds arrive as 256 MiB windows, so a Worker that
answers `200` with a whole 245 GiB body wedges every pull — and `check` fails that row rather
than reporting a reachable bucket. Both probes live in `src/storage/mod.rs` with tests that
assert they *fail* on a 200-to-everything endpoint.

To point them at a Worker before any manifest names it, deploy it and repoint one manifest's
`base_uri` (below) on a branch; `snapshot verify` reads each manifest's own `base_uri`.

## Repoint

Already done — all eight manifests and `BASE_URI` name the Worker above. This section is the
procedure for *moving* the read path, which is also the only escape hatch if the Worker goes
down, since there is no fallback in code:

```sh
OLD=$(sed -n 's/^base_uri *= *"\(.*\)"/\1/p' ../../snapshots/testnet/zebra-6.2.3-sapling.toml)
sed -i "s|$OLD|$BASE|" ../../snapshots/*/*.toml
$EDITOR ../../src/storage/mod.rs      # BASE_URI — stamps every manifest generated later
```

Both, or neither: committed manifests keep the old host until edited, and `BASE_URI` only
affects manifests generated afterwards. `ztest snapshot verify` reads each manifest's own
`base_uri`, so it catches a half-done move.

## Operate

```sh
npx wrangler tail                     # live request log
npx wrangler deployments status       # what is live
npx wrangler rollback                 # previous version
npx wrangler delete                   # remove it (repoint the manifests first)
```

## Push credentials

Only `ztest snapshot push` needs them, and only on the machine publishing a fixture:

```sh
ztest snapshot config set             # prompts; secret is not echoed
ztest snapshot config show            # secret shown as <n chars>
```

Non-interactive (CI):

```sh
printf '%s' "$R2_SECRET" | ztest snapshot config set \
  --endpoint https://<account-id>.r2.cloudflarestorage.com \
  --bucket ztest-archives --access-key-id "$R2_KEY_ID" --secret-access-key -
```

The file is `~/.config/ztest/bucket.toml`, mode `0600`. `config set` proves the credentials
against the bucket before returning, so a typo fails in seconds rather than at the end of a
multi-hour push.

Scope the R2 API token to **Object Read & Write on `ztest-archives` alone** — never an
account-wide token. The Worker needs no token at all: it reaches the bucket through a
binding, so nothing here or in `wrangler.jsonc` is a secret.

## Do not

- **Put this behind Cache Everything or a cache rule.** Cloudflare answers a `Range` by
  stripping the header, pulling the *whole* body from the Worker, then slicing. Against a
  245 GiB seed that is pathological, and past the cacheable size limit (512 MB Free/Pro) it
  cannot store the result anyway. Responses carry `cache-control: no-store` to make the
  misconfiguration inert; do not remove it.
- **Add write verbs.** `push` goes to the S3 endpoint with credentials. This Worker is the
  read path and has no business accepting a `PUT`.
- **Widen the key pattern.** It serves `lfs/<64 hex>` and 404s everything else, so the
  bucket cannot become a public filesystem by accident.

## Limits that matter here

| Workers Free  | limit        | one 245 GiB pull |
| ------------- | ------------ | ---------------- |
| requests      | 100,000/day  | ~980             |
| CPU time      | 10 ms        | ~0 (streaming)   |
| memory        | 128 MB       | ~0 (streaming)   |
| response body | no limit     | —                |
