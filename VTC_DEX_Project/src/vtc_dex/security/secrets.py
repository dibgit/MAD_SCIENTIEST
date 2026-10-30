"""Access environment-provided secrets without logging their values."""

import os


def get_secret(name: str) -> str | None:
    return os.getenv(name)
