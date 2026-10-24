# Crash-replay (simulated power cuts)

Replays a `dm-log-writes` log of a real XStore SYNC run to many crash points and checks that every acknowledged call survives
fsck + mount + server restart (WAL replay). Needs root on the server host, `dm_log_writes` in the kernel, and the
`xstore_zmq_fault_client` / `xstore_zmq_insert_client` binaries on a client node (`tests/perf/`).

| File | Role |
|---|---|
| `logw_ctl.sh` | server host: 6 GB ext4 image on loop -> dm-log-writes (log on a 40 GB image), mount, finish, mount a replayed image, teardown (idempotent, never kills anything) |
| `mark_loop.sh` | server host, root: a `t<epoch_ms>` mark into the dm log every ~20 ms |
| `replay.py` | parses the dm-log-writes format and writes crash states (A, B, C0, C1) per mark as sparse images; `replay.py info <log>` summarises a log |
| `logw_run.py` | orchestrator, run from a workstation: `logw_run.py <out> <name> <new\|old> <rows list> <seconds> <K points>` (hosts, paths and binaries are constants at its top; `XS_SSH` is the ssh command, called as `<cmd> <host> '<remote command>'`) |

States per crash point (mark time `t`): A = every write logged before the mark; B = only writes up to the last FLUSH/FUA (the
unflushed ones are lost); C0/C1 = B plus a random half (seeded) of the writes in flight. Required to be present: every call
acked at least 100 ms before `t` (client and server clocks are NTP-synced). The run ends with a negative control, the earliest
state checked against ALL acks, which must report missing calls.

Not modelled: writes torn inside one bio, reordering across a flush boundary, ASYNC (an ASYNC ack promises no durability).
Results and method notes: TODO.md, entry "Validate durability on real hardware".
