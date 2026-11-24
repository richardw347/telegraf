# Docker startup timeout reproducer

This synthetic example exercises a Docker startup timeout while a UDP service
input has filled Telegraf's startup metric channel. It needs Python 3 and a
Telegraf binary on Linux; it does not need a Docker daemon, containers, remote
services, credentials, or third-party Python packages.

In one terminal, from this directory:

```sh
python3 trigger.py
```

In another terminal:

```sh
telegraf --config listener.conf --config docker.conf
```

Use these two config arguments in this order. Loading the inputs from separate
files makes socket_listener start before Docker; putting both inputs in one
file does not guarantee that ordering.

The fake Docker endpoint sends one UDP packet containing 101 line-protocol
metrics, then takes longer to respond than Docker's one-second timeout. The
startup channel holds 100 metrics and its consumer has not started yet, so the
parser blocks on metric 101.

## Expected behavior

With `startup_error_behavior = "retry"`, a temporary Docker timeout should let
the agent finish starting, deliver the UDP metrics and keep retrying Docker.
When Docker recovers, Docker metric collection should resume in the same process.

## Observed behavior on stock releases

Telegraf 1.38.4 and 1.40.4 leave the process alive, close the UDP listener, and
write no metrics. Docker's startup timeout is not classified as retryable, even
though `retry` is configured. The resulting startup-error cleanup waits on a
parser that is blocked sending to the unconsumed metric channel.

For a stack dump, send `kill -QUIT <pid>` to **only the disposable Telegraf
process started for this example**. This terminates that process and prints
the Go stacks. The blocked paths include:

- `Agent.startInputs` → `stopRunningInputs` → `SocketListener.Stop` →
  `packetListener.close` → `WorkerPool.StopAndWait`.
- The UDP parser → `accumulator.AddMetric`, blocked on a channel send.

Stop the Python server with Ctrl-C when finished. Do not run this deliberate
failure trigger against a production agent.

## Scope of the source fix

The source patch treats `context.DeadlineExceeded` from either Docker's ping
or info request as retryable. With this example's `retry` policy, UDP collection
starts while Docker remains unavailable. Docker is kept registered and can
recover when its endpoint responds again.

The helper intentionally stays unavailable. It demonstrates startup and other
input delivery; the Go regression test additionally switches the same Docker
endpoint back online and verifies CPU and memory metrics.

This patch does not fix the separate generic startup-cleanup deadlock. Changing
the policy here to `error` can still trigger that deadlock after the patch.
It also does not prove what triggered any particular production incident.

The reproducer is on a separate branch so it can be reviewed and run without
adding example tooling to the proposed two-file source fix.

## Repeated validation

Linux amd64 results on 24 November 2026:

- Official 1.38.4 (`c79b06d58e912124624d029a88bbe182254f0ff4`):
  **100/100** startup hangs, each confirmed with blocked Go stacks, an absent
  owned UDP listener, and zero output.
- Official 1.40.4 (`c1b3d7f70745dd8a054256b05b421606d0077995`):
  **100/100** with the same confirmed failure.
- No-traffic controls: **20/20 per release** exited with status 1 instead of
  hanging. The Docker endpoint and timeout were otherwise unchanged.
- Current master `620deb84f26eec49a6bc8fe776975b3112926933` plus the proposed
  Docker timeout patch: **100/100** delivered all 101 UDP metrics while Docker
  was unavailable, then resumed Docker CPU and memory metrics in the same
  process. This was a test-only binary built with the Docker and socket_listener
  inputs, file output, and Influx parser/serializer.
- `TestStartupTimeoutRetry` fails on unpatched master at both the ping and info
  startup requests. With the patch, both cases passed 100 repetitions, including
  recovery on the same endpoint. The complete Docker package passed normally
  and with the race detector; package lint and vet, and repository formatting
  checks passed with Go 1.27.0 and golangci-lint 2.15.0.
- The exact `trigger.py` and two config files above were also checked unchanged
  against stock 1.40.4 and the patched test binary.

These are finite observed results under a controlled trigger, not a guarantee
about all environments or proof of an original production incident's cause.
The initial stress harness sampled `/proc` too early in one trial; the final
runs above use the included checker, which waits past the Docker deadline
before checking shutdown state.

`verify.py` is optional repeat-test tooling, separate from the minimal trigger.
It randomizes loopback ports, shortens collection/flush intervals to 100 ms,
keeps the one-second Docker timeout, and uses four independent child processes
at a time. It captures the failing stacks before terminating each local child.
Set `TEST_ARTIFACTS` to an existing directory on your test-storage volume, then
use a fresh output directory for each run:

```sh
python3 verify.py /path/to/stock/telegraf "$TEST_ARTIFACTS/stock-failure" \
  --config . --mode broken --count 100
python3 verify.py /path/to/stock/telegraf "$TEST_ARTIFACTS/no-traffic" \
  --config . --mode control --count 20
python3 verify.py /path/to/patched/telegraf "$TEST_ARTIFACTS/patched-recovery" \
  --config . --mode patched --count 100
```

Each run writes `results.json` plus per-trial configuration, output and logs.
No production configuration or data is used.
