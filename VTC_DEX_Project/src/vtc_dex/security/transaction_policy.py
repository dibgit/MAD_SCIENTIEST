"""Policy checks for the currently supported mock-only execution mode."""


def validate_execution_mode(mode: str) -> None:
    if mode != "MOCK":
        raise ValueError(
            "Only EXECUTION_MODE=MOCK is available; "
            "no live swap adapter is implemented."
        )
