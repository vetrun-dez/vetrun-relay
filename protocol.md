# Vet Run online relay protocol (v0.21.25)

Transport: JSON text frames over WebSocket. Same room-relay design as Shift Pass
(server/relay_server.gd there), re-implemented as a tiny Node server so it can be
hosted for free. The relay knows rooms and peers only; it never reads game data.

Room code: 5 chars, 3 letters + 2 digits, no I/L/O/0/1 (e.g. DOG42, KTZ37).

Peer -> relay
  {"t":"host","v":1,"game":"vetrun"}            create a room; sender is host
  {"t":"join","v":1,"code":"DOG42"}             join a room
  {"t":"send","to":"host"|"all"|<id>,"data":{}} forward to room peers
  {"t":"ping"}                                   keep-alive (relay answers pong)
Relay -> peer
  {"t":"hosted","code":"DOG42","peer":1}
  {"t":"joined","code":"DOG42","peer":2,"host":1}
  {"t":"peer_joined","peer":2} / {"t":"peer_left","peer":2}   (to host)
  {"t":"msg","from":<id>,"data":{}}
  {"t":"room_closed"}   (to guest when host leaves)
  {"t":"error","reason":"bad_code|no_such_room|room_full|version_mismatch|..."}
  {"t":"pong"}
