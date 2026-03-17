class DomainError(Exception):
    """Base for all domain rule violations."""
    pass


class OptimisticConcurrencyError(DomainError):
    """Raised when expected_version doesn't match current stream version."""

    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Stream '{stream_id}': expected version {expected}, got {actual}"
        )