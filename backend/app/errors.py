"""Typed broker errors.

Every error carries an HTTP status, a stable machine-readable `code` and a
`retryable` flag. They travel unchanged between brokers (leader -> entry
broker) and all the way to the frontend, which uses `code` / `retryable`
to decide between "retry safely", "use a new order id" or "show failure".
"""


class BrokerError(Exception):
    status_code = 500
    code = "BROKER_ERROR"
    retryable = False

    def __init__(self, message: str = "", **extra):
        super().__init__(message)
        self.message = message
        self.extra = extra

    def to_dict(self):
        return {"detail": self.message, "code": self.code, "retryable": self.retryable, **self.extra}


class NotLeader(BrokerError):
    status_code, code, retryable = 409, "NOT_LEADER", True


class StaleEpoch(BrokerError):
    status_code, code, retryable = 409, "STALE_EPOCH", True


class NotEnoughReplicas(BrokerError):
    status_code, code, retryable = 503, "NOT_ENOUGH_REPLICAS", True


class NoAvailableLeader(BrokerError):
    status_code, code, retryable = 503, "NO_AVAILABLE_LEADER", True


class OffsetGap(BrokerError):
    status_code, code, retryable = 409, "OFFSET_GAP", True


class LogDivergence(BrokerError):
    status_code, code, retryable = 409, "LOG_DIVERGENCE", True


class IdempotencyConflict(BrokerError):
    status_code, code, retryable = 409, "IDEMPOTENCY_CONFLICT", False


class TransactionAborted(BrokerError):
    status_code, code, retryable = 409, "TRANSACTION_ABORTED", False


class PeerUnreachable(BrokerError):
    status_code, code, retryable = 503, "PEER_UNREACHABLE", True


class PeerHTTPError(BrokerError):
    status_code, code, retryable = 502, "PEER_ERROR", False


_BY_CODE = {
    cls.code: cls
    for cls in (
        NotLeader, StaleEpoch, NotEnoughReplicas, NoAvailableLeader, OffsetGap,
        LogDivergence, IdempotencyConflict, TransactionAborted,
    )
}


def error_from_payload(status: int, body: dict) -> BrokerError:
    """Rebuild a typed error from a peer broker's JSON error response."""
    body = body if isinstance(body, dict) else {}
    detail = body.get("detail", "")
    if not isinstance(detail, str):
        detail = str(detail)
    cls = _BY_CODE.get(body.get("code"))
    extra = {k: v for k, v in body.items() if k not in ("detail", "code", "retryable")}
    if cls is not None:
        return cls(detail, **extra)
    error = PeerHTTPError(detail or f"HTTP {status}", **extra)
    error.status_code = status
    return error
