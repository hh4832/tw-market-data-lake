"""Typed errors. Callers branch on these instead of parsing messages."""


class LakeError(Exception):
    """Base class for all tw_lake errors."""


class ConfigError(LakeError):
    pass


class ProtectedPathError(LakeError):
    """Attempt to write outside the folders this repo owns."""


class LockHeldError(LakeError):
    """Another writer holds the single-writer lock."""


class LockLostError(LakeError):
    """The lock file no longer belongs to this run (stolen or removed)."""


class IntegrityError(LakeError):
    """Read-back / SHA256 / content-hash verification failed."""


class ImmutableViolation(LakeError):
    """An operation would overwrite an immutable published file."""


class ParamsMismatchError(LakeError):
    """Request parameters / schema differ from the published dataset (cache key changed)."""


class RequiredDatasetMissing(LakeError):
    """A required upstream dataset (e.g. FinLab disposal history) could not be loaded."""


# ---- API errors -----------------------------------------------------------

class ApiError(LakeError):
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class ApiNotFound(ApiError):
    """HTTP 404. Meaning is ambiguous and must be classified, never assumed complete."""


class ApiBadRequest(ApiError):
    """HTTP 400. Usually a parameter problem; not retried."""


class ApiRateLimited(ApiError):
    """HTTP 429."""


class ApiTransient(ApiError):
    """5xx, connection reset, timeout... retried with backoff."""


class RetriesExhausted(ApiError):
    pass


class RequestBudgetExceeded(LakeError):
    """--max-requests reached; the run stops cleanly and leaves remaining windows pending."""
