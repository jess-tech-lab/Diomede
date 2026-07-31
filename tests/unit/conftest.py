import os

# Set required env vars before any test module imports service code that
# validates them at module level (e.g. src.edge.forwarder, src.orchestrator.main).
os.environ.setdefault("ORCHESTRATOR_API_KEY", "test-key")
os.environ.setdefault("AGENT_ID", "test-agent")
os.environ.setdefault("ORCHESTRATOR_BASE", "http://orchestrator:8000")
os.environ.setdefault("REGION1_NAME", "us-east1")
os.environ.setdefault("REGION2_NAME", "eu-west1")
os.environ.setdefault("REGION3_NAME", "asia-northeast1")
os.environ.setdefault("REGION4_NAME", "af-south1")
