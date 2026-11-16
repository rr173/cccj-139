"""Error taxonomy for the multi-target loader.

The retry contract (see README.md) is defined in terms of these types:

* :class:`RetryableError` - serialization failure / deadlock.  The caller may
  re-submit the *same* batch object; digest-based idempotency makes this safe
  even when the previous attempt's outcome is ambiguous.
* :class:`PositionConflict`, :class:`StaleExpectedPrevious`,
  :class:`BatchError`, :class:`KeyConflict`, :class:`BatchFailed` -
  deterministic rejections.  Retrying the identical batch will fail again;
  the caller must fix the batch (or re-sync progress) first.
"""


class LoaderError(Exception):
    """Base class for all loader errors."""


class ConfigError(LoaderError):
    """Route configuration is invalid or incompatible with the database."""


class UnknownPartition(LoaderError):
    """Batch targets a partition that was never declared."""


class PositionConflict(LoaderError):
    """Batch position clashes with the recorded history of the partition.

    Raised when the same position already holds a *different* digest, when a
    position is already buried in the applied history, or when the first
    batch does not start at the declared start position.
    """


class StaleExpectedPrevious(LoaderError):
    """``expected_previous`` does not match the partition's last position."""


class BatchError(LoaderError):
    """The batch itself is malformed or violates stream semantics.

    Examples: insert of an existing row_id, update/delete of an unknown
    row_id, non-JSON-scalar field values.
    """


class KeyConflict(LoaderError):
    """Final state of the operation collides on the business unique key.

    The whole batch / reroute is rejected and rolled back.  The message
    carries row_ids and key *digests* only, never raw business values.
    """


class BatchFailed(LoaderError):
    """The transaction failed mid-way (e.g. a SQL data error).

    Everything was rolled back: no target rows, no ownership rows, no
    position movement.  Not retryable as-is.
    """


class RetryableError(LoaderError):
    """Serialization failure or deadlock; the same batch may be retried."""
