# Faultstorm

Run `make faultstorm` to build the MySQL 8.0 test cluster, inject network partitions, process kills and freezes, switchovers, maintenance and resetups while writing, and check that confirmed writes survive. Set `VERSION=5.7`, `8.4`, or `9.7` to use another version; CI runs all four. Resetup can wipe any MySQL node, including the master; the daemon restores it asynchronously. PR runs write for 3 minutes and check for 1 minute; scheduled runs use 30 and 5 minutes. Logs and the replayable scenario are saved in `tests/logs/faultstorm/`.

Each cycle injects one fault or a combination of faults on one host. After the faults are healed, the test waits for one writable master, both replicas to catch up, and semi-sync to resume. The checker then compares the last successful read from each MySQL host.

For a shorter local run: `VERSION=8.0 sh tests/faultstorm/run.sh --write-duration 60 --read-duration 30`.

To replay a failed run, save a copy of `tests/logs/faultstorm/scenario.log` before rerunning and pass its path inside the container, for example `--replay-scenario /logs/failed-scenario.log`.
