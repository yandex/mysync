# Faultstorm

Run `make faultstorm` to build the MySQL 8.0 test cluster, inject random network partitions while writing, and check that confirmed writes survive. Logs and the replayable scenario are saved in `tests/logs/faultstorm/`.

For a shorter local run: `VERSION=8.0 sh tests/faultstorm/run.sh --write-duration 60 --read-duration 30`.

To replay a failed run, save a copy of `tests/logs/faultstorm/scenario.log` before rerunning and pass its path inside the container, for example `--replay-scenario /logs/failed-scenario.log`.
