# vetrun-relay

Tiny WebSocket relay for Vet Run online co-op room codes (e.g. `DOG42`). Host = dog, guest = cat.
It only knows rooms and peers; it never reads game data. Wire protocol: [protocol.md](protocol.md).

## Deno Deploy (current host, free tier)

- Entrypoint: `main.ts` (no build step, no dependencies, no env vars needed)
- Health check: `GET /` -> `vetrun relay ok, rooms=N`
- The game connects to `wss://<app>.deno.dev` (set `DEFAULT_RELAY_URL` in the game, or `?relay=` to override)
- Tip: pin the app to one region (e.g. `us`) so a host and guest usually share an instance (see Notes)

Run locally:

```sh
deno run --allow-net --allow-env main.ts          # PORT env var, default 9080
deno run --allow-net test/clients.ts ws://127.0.0.1:9080            # protocol test
deno run -A test/two_isolates.ts 9181 9182 &                        # two isolates linked by BroadcastChannel
deno run --allow-net test/clients.ts ws://127.0.0.1:9181 ws://127.0.0.1:9182
deno run -A test/isolate_loss.ts                                    # an isolate vanishes (~60 s)
```

## Notes

- The isolate where the host connects owns the room. Other isolates reach it over one shared
  `BroadcastChannel` ("vetrun-relay-v1"): join requests, forwarded game data, leave/close, and a
  15 s heartbeat so a vanished isolate's players get `peer_left` / `room_closed`.
- Peer ids are random integers (not 1, 2, ...) so they stay unique across isolates.
- BroadcastChannel always links isolates in the same process. Whether it also links separate
  Deno Deploy instances depends on the platform; if it does not, a guest that lands on a
  different instance gets `no_such_room` and can simply retry.

## Legacy Node version

`relay.js` is the original Node (`ws`) relay with the same protocol: `npm install && node relay.js`.
