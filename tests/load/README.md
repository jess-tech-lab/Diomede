# Load tests (Locust)

## Setup

```bash
pip install -e ".[load,scripts]"     # locust + pydicom (for synthetic DICOM)
docker compose up -d --build         # the stack must be running
```

Configuration is read from `.env`:
`ORCHESTRATOR_HTTPS_URL`, `EDGE_AGENT1_HTTPS_URL`, `ORCHESTRATOR_API_KEY`,
`EDGE_AGENT1`, `ORTHANC_USER`/`ORTHANC_PASSWORD`. TLS uses the local dev CA
(`certs/ca.pem`) automatically; set `LOAD_VERIFY_TLS=false` to skip verification.

## Run

```bash
# Routing-latency KPI: 100 concurrent clients for 1 minute
locust --config tests/load/locust.conf RoutingUser --users 100 --run-time 1m

# Transfer-success KPI: a 100-image burst (stops after 100 POSTs)
LOAD_BURST_COUNT=100 locust --config tests/load/locust.conf IngestUser --users 10

# Interactive web UI instead of headless (drop --headless via CLI):
locust -f tests/load/locustfile.py RoutingUser --host "$ORCHESTRATOR_HTTPS_URL"
```

Tunable via env: `LOAD_FILE_SIZE_KB` (image size, default 50),
`LOAD_MAX_P99_MS` (default 50), `LOAD_MAX_FAIL_RATIO` (default 0.005).

## Notes

- `IngestUser` success = the edge **accepted** the instance (client → edge). True
  end-to-end delivery (edge → cloud node) is measured by
  [`src/benchmark/success_rate.py`](../../src/benchmark/success_rate.py), which polls
  the cloud nodes for arrivals.
- **Failover** (node offline mid-transfer) is timed by
  [`src/benchmark/failover_time.py`](../../src/benchmark/failover_time.py). To observe
  failover under load, start an `IngestUser` burst and `docker stop` a cloud node
  mid-run; the transfer-success KPI reflects the impact.
