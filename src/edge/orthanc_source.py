"""
edge/orthanc_source.py – DicomSource backed by the Edge Orthanc REST API.

Polls GET /instances for NewInstance events, streams raw DICOM bytes via
GET /instances/{id}/file, and acknowledges by deleting the local copy.
"""

from __future__ import annotations

import httpx
from dotenv import load_dotenv

from src.edge.transport import DicomSource
from src.utils.env import require_env
from src.utils.logging_config import get_logger

log = get_logger(__name__, "ORTHANC_SOURCE")
load_dotenv()

_EDGE_BASE = require_env("EDGE_BASE")
_EDGE_AUTH = (require_env("EDGE_USER"), require_env("EDGE_PASS"))


class OrthancSource(DicomSource):
    """Reads new DICOM instances from a co-located Edge Orthanc via its REST API."""

    def __init__(
        self,
        base: str = _EDGE_BASE,
        auth: tuple[str, str] = _EDGE_AUTH,
    ) -> None:
        self._base = base.rstrip("/")
        self._auth = auth
        self._last_seq: int = 0

    async def poll_new(self, client: httpx.AsyncClient) -> list[str]:
        """Return all instance IDs currently in the Edge Orthanc buffer."""
        resp = await client.get(
            f"{self._base}/instances",
            auth=self._auth,
            timeout=10,
        )
        resp.raise_for_status()
        log.info("New instances: %s", resp.json())
        list_response: list[str] = resp.json()
        return list_response

    @asynccontextmanager
    async def open_stream(
        self, client: httpx.AsyncClient, instance_id: str
    ) -> AsyncIterator[AsyncIterator[bytes]]:
        """Stream raw DICOM bytes for instance_id from Edge Orthanc.

        Uses httpx streaming so the file is never fully buffered in memory -- the
        caller pipes the yielded iterator straight to the destination node.
        """
        async with client.stream(
            "GET",
            f"{self._base}/instances/{instance_id}/file",
            auth=self._auth,
            timeout=60,
        ) as resp:
            resp.raise_for_status()
            yield resp.aiter_bytes()

    async def acknowledge(self, client: httpx.AsyncClient, instance_id: str) -> None:
        """Delete the instance from Edge Orthanc to prevent disk fill."""
        resp = await client.delete(
            f"{self._base}/instances/{instance_id}",
            auth=self._auth,
            timeout=10,
        )
        resp.raise_for_status()
        log.info("instance=%s deleted from edge buffer", instance_id)
