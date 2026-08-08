"""
Locust load tests for the Diomede orchestrator and edge ingestion path.

Two independent scenarios, each mapping to a proposal KPI (Section 7.1):

  RoutingUser  -> GET /get-best-node latency under concurrency
                  KPI: routing latency p99 < 50 ms @ 100 concurrent requests
  IngestUser   -> POST /instances DICOM ingestion at the edge Orthanc
                  KPI: >= 99.5% transfer success over a 100-image burst

Run one scenario at a time by naming its class (clean per-KPI numbers):

    locust --config tests/load/locust.conf RoutingUser --users 100 --run-time 1m
    LOAD_BURST_COUNT=100 locust --config tests/load/locust.conf IngestUser --users 10

Configuration (base URLs, API key, credentials, CA cert) is read from the same
.env the rest of the stack uses. See tests/load/README.md.
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import gevent
from dotenv import load_dotenv
from locust import HttpUser, between, events, task

from src.simulator.generate_dicom import make_sized

load_dotenv(override=True)

# --- Targets and credentials (host-facing; the simulator runs outside docker) ---
ORCH_URL = os.environ.get("ORCHESTRATOR_HTTPS_URL", "https://localhost:8000")
EDGE_URL = os.environ.get("EDGE_AGENT1_HTTPS_URL", "https://localhost:8046")
API_KEY = os.environ.get("ORCHESTRATOR_API_KEY", "")
AGENT_ID = os.environ.get("EDGE_AGENT1", "agent-001")
EDGE_AUTH = (
    os.environ.get("ORTHANC_USER", "orthanc"),
    os.environ.get("ORTHANC_PASSWORD", "orthanc"),
)
FILE_SIZE_KB = int(os.environ.get("LOAD_FILE_SIZE_KB", "50"))
# Stop IngestUser after this many POSTs (the "100-image burst"); 0 = run until
# --run-time. Locust has no --iterations flag, so the burst is bounded here.
BURST_COUNT = int(os.environ.get("LOAD_BURST_COUNT", "0"))
_ingest_sent = 0

# --- KPI thresholds (Section 7.1); override via env for experimentation ---
MAX_P99_MS = float(os.environ.get("LOAD_MAX_P99_MS", "50"))
MAX_FAIL_RATIO = float(os.environ.get("LOAD_MAX_FAIL_RATIO", "0.005"))

# Stable stat names so the KPI check can find each endpoint whether the scenarios
# run alone or together.
_ROUTE_NAME = "GET /get-best-node"
_INGEST_NAME = "POST /instances"


def _tls_verify() -> str | bool:
    """Resolve TLS verification for the self-signed dev CA.

    REQUESTS_CA_BUNDLE points at the in-container path (/certs/ca.pem), which does
    not exist on the host, so resolve explicitly: an override, then the repo-local
    CA, then fall back to skipping verification for local load runs.
    """
    override = os.environ.get("LOAD_CA_CERT")
    if override:
        return override
    if os.environ.get("LOAD_VERIFY_TLS", "").lower() in ("0", "false", "no"):
        return False
    local_ca = Path("certs/ca.pem")
    return str(local_ca) if local_ca.exists() else False


_VERIFY = _tls_verify()


class RoutingUser(HttpUser):
    """Hammer the orchestrator routing decision path (control-plane latency KPI)."""

    host = ORCH_URL
    wait_time = between(0, 0.05)

    def on_start(self) -> None:
        self.client.verify = _VERIFY
        self.client.headers.update({"X-API-Key": API_KEY})

    @task(8)
    def get_best_node(self) -> None:
        with self.client.get(
            "/get-best-node",
            params={"agent_id": AGENT_ID},
            name=_ROUTE_NAME,
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:200]}")

    @task(1)
    def list_nodes(self) -> None:
        self.client.get("/nodes", name="GET /nodes")


class IngestUser(HttpUser):
    """Post synthetic DICOM to the edge Orthanc (data-plane success-rate KPI).

    Success here means the edge accepted the instance (client -> edge buffering).
    True end-to-end delivery (edge -> cloud node) is measured separately by
    src/benchmark/success_rate.py, which polls the cloud nodes for arrivals.
    """

    host = EDGE_URL
    wait_time = between(0, 0.05)

    def on_start(self) -> None:
        self.client.verify = _VERIFY
        self.client.auth = EDGE_AUTH

    @task
    def post_instance(self) -> None:
        global _ingest_sent
        if BURST_COUNT and _ingest_sent >= BURST_COUNT:
            return  # burst complete; a quit() is already scheduled
        _ingest_sent += 1  # reserve a slot; check has no yield -> exact count
        ds = make_sized(FILE_SIZE_KB, random_pixels=True)
        buf = io.BytesIO()
        ds.save_as(buf)
        with self.client.post(
            "/instances",
            data=buf.getvalue(),
            headers={"Content-Type": "application/dicom"},
            name=_INGEST_NAME,
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:200]}")
        if BURST_COUNT and _ingest_sent >= BURST_COUNT:
            # Burst done: stop the run from a fresh greenlet so this request
            # finishes cleanly (quit() kills user greenlets, including this one).
            gevent.spawn_later(0, self.environment.runner.quit)


@events.test_start.add_listener
def _reset_burst(environment, **_kwargs) -> None:
    """Reset the burst counter so repeated runs in one process (web UI) start fresh."""
    global _ingest_sent
    _ingest_sent = 0


@events.test_stop.add_listener
def _check_kpis(environment, **_kwargs) -> None:
    """After the run, evaluate each measured endpoint against its KPI and set a
    non-zero exit code on failure so a headless/CI run reflects the result."""
    stats = environment.stats
    failures: list[str] = []

    route = stats.get(_ROUTE_NAME, "GET")
    if route.num_requests:
        p99 = route.get_response_time_percentile(0.99)
        verdict = "PASS" if p99 <= MAX_P99_MS else "FAIL"
        if p99 > MAX_P99_MS:
            failures.append(f"routing p99 {p99:.1f}ms > {MAX_P99_MS:.0f}ms")
        print(f"[KPI] routing latency p99 = {p99:.1f} ms (limit {MAX_P99_MS:.0f}) -> {verdict}")

    ingest = stats.get(_INGEST_NAME, "POST")
    if ingest.num_requests:
        ratio = ingest.num_failures / ingest.num_requests
        verdict = "PASS" if ratio <= MAX_FAIL_RATIO else "FAIL"
        if ratio > MAX_FAIL_RATIO:
            failures.append(f"ingest failure ratio {ratio:.4f} > {MAX_FAIL_RATIO:.4f}")
        pct, limit = (1 - ratio) * 100, (1 - MAX_FAIL_RATIO) * 100
        print(f"[KPI] transfer success = {pct:.2f}% (limit {limit:.1f}%) -> {verdict}")

    if failures:
        print("[KPI] LOAD TEST FAILED: " + "; ".join(failures))
        environment.process_exit_code = 1
    else:
        print("[KPI] all measured KPIs within target")
