import os


def require_env(name: str) -> str:
    """Return the environment variable's value, or raise if it's unset/empty."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} environment variable must be set")
    return value
