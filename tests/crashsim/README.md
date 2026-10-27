# Crash-replay (simulated power cuts) and I/O-error injection

Replays a `dm-log-writes` log of a real XStore SYNC run to many crash points and checks that every acknowledged call survives
fsck + mount + server restart (WAL replay). Needs root on the server host, `dm_log_writes` in the kernel, and the
`xstore_zmq_fault_client` / `xstore_zmq_insert_client` binaries on a client node (`tests/perf/`).

| File | Role |
|---|---|
| `logw_ctl.sh` | server host: 6 GB ext4 image on loop -> dm-log-writes (log on a 40 GB image), mount, finish, mount a replayed image, teardown (idempotent, never kills anything) |
| `mark_loop.sh` | server host, root: a `t<epoch_ms>` mark into the dm log every ~20 ms |
| `replay.py` | parses the dm-log-writes format and writes crash states (A, B, C0, C1) per mark as sparse images; `replay.py info <log>` summarises a log |
| `logw_run.py` | orchestrator, run from a workstation: `logw_run.py <out> <name> <head\|new\|old> <rows list> <seconds> <K points> [SYNC\|ASYNC]` (hosts, paths and binaries are constants at its top; `XS_SSH` is the ssh command, called as `<cmd> <host> '<remote command>'`) |

States per crash point (mark time `t`): A = every write logged before the mark; B = only writes up to the last FLUSH/FUA (the
unflushed ones are lost); C0/C1 = B plus a random half (seeded) of the writes in flight. Required to be present: every call
acked at least 100 ms before `t` (client and server clocks are NTP-synced). The run ends with a negative control, the earliest
state checked against ALL acks, which must report missing calls.

Crash points are one seeded random point per time stripe. ASYNC mode (an ASYNC ack promises no durability; every 8th ack is checked): a missing call is
reported, not an error; the run fails only on fsck/mount/restart errors and on wrong or unanswered rows. Each state is verified
three times, against the acks at least 100 ms, 1 s and 5 s older than the mark, so the output shows how old an ack must be
before nothing is missing.

Not modelled: writes torn inside one bio, reordering across a flush boundary.
Results and method notes: TODO.md, entry "Validate durability on real hardware".

## I/O-error injection (`fault_run.py`, `fault_ctl.sh`)

The other half of the durability check: the data directory sits on loop -> dm -> ext4 and, mid-run, the device is switched to the
dm `error` target (`perm`: stays failed; `trans`: healthy again after 2 s; `none`: control). Then the server is stopped, fsck + remount +
restart (WAL replay), and every acked call is verified present, as in the crash replay. Same prerequisites (root on the server host,
the fault client binaries on a client node); `fault_ctl.sh` is copied to `/NVME_RAID-0/faultimg/` on the server host, and the run
cannot overlap a crash-replay run (both mount `/mnt/xsfault`).

`fault_run.py <out> <name>:<head|new|old>:<mount opts or ->:<none|perm|trans>[:<rows per call>[:<SYNC|ASYNC>]] ...` (the same
`XS_SSH` and constants-at-the-top convention as `logw_run.py`).
