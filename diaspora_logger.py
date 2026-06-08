"""Diaspora event-fabric logging handler.

Backend-neutral :class:`logging.Handler` that publishes every record to a
Kafka topic via the Diaspora event SDK. Register once at process start
with :func:`set_diaspora_logger`; works under Parsl, Dask, Ray, or any
plain Python process.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable
from typing import Any

from academy.logging.configs.base import LogConfig

DEFAULT_FORMAT = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d %(process)d %(threadName)s [%(levelname)s] %(message)s"


def resolve_kafka_topic(topic_name: str, namespace: str) -> str:
    """Expand a short topic name to ``namespace.topic`` if not already qualified."""
    return topic_name if "." in topic_name else f"{namespace}.{topic_name}"

def get_ssl_cafile() -> str | None:
    """Return certifi's CA bundle path if available, else ``None``."""
    try:
        import certifi

        return certifi.where()
    except ImportError:
        return None


def _json_safe(value: Any) -> Any:
    """Coerce a ``LogRecord.__dict__`` value to something JSON-serializable."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return repr(value)


class DiasporaHandler(logging.Handler):
    """Logging handler that publishes records to a Diaspora Kafka topic."""

    def __init__(self, producer: Any, kafka_topic: str, send_timeout: int = 30) -> None:
        super().__init__()
        self.producer = producer
        self.kafka_topic = kafka_topic
        self.send_timeout = send_timeout

    def emit(self, record: logging.LogRecord) -> None:
        try:
            # Format first so asctime is populated on the record before
            # we copy record.__dict__ into the event.
            formatted = self.format(record) if self.formatter is not None else None
            event = {k: _json_safe(v) for k, v in record.__dict__.items()}
            event["message"] = record.getMessage()
            if formatted is not None:
                event["formatted"] = formatted
            self.producer.send(self.kafka_topic, event)
        except Exception:  # noqa: BLE001
            # ``logging.Handler.emit`` contract (stdlib): any exception
            # must be routed through ``self.handleError`` so a broken
            # handler doesn't crash the logger. Narrowing would violate
            # the contract.
            self.handleError(record)

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.producer.flush(timeout=self.send_timeout)
        super().close()


def set_diaspora_logger(
    topic_name: str = "diaspora-events",
    name: str = "root",
    level: int = logging.INFO,
    format_string: str | None = None,
    send_timeout: int = 30,
    max_block_ms: int = 1000,
) -> Callable[[], None]:
    """Attach a :class:`DiasporaHandler` to the named logger.

    Mirrors the shape of :func:`parsl.set_file_logger`; returns an unregister
    callback that removes the handler and closes the underlying Kafka
    producer when invoked.
    """
    try:
        from diaspora_event_sdk import Client as GlobusClient
        from diaspora_event_sdk.sdk.kafka_client import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "diaspora-event-sdk is required. Install with: pip install diaspora-event-sdk",
        ) from exc

    client = GlobusClient()
    client.create_key()
    create_topic_name = topic_name.split(".", 1)[1] if "." in topic_name else topic_name
    topic_result = client.create_topic(create_topic_name)
    if isinstance(topic_result, dict):
        status = topic_result.get("status")
        if status not in {"success", "no-op", None}:
            raise RuntimeError(f"create_topic failed with status={status!r}")

    kafka_topic = resolve_kafka_topic(topic_name, client.namespace)
    ssl_cafile = get_ssl_cafile()
    producer_kwargs: dict[str, Any] = {"max_block_ms": max_block_ms}
    if ssl_cafile:
        producer_kwargs["ssl_cafile"] = ssl_cafile
    producer = KafkaProducer(kafka_topic, **producer_kwargs)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = DiasporaHandler(producer, kafka_topic, send_timeout)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(format_string or DEFAULT_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"),
    )
    logger.addHandler(handler)

    def unregister() -> None:
        logger.removeHandler(handler)
        with contextlib.suppress(Exception):
            handler.close()
        if hasattr(producer, "close"):
            with contextlib.suppress(Exception):
                producer.close(timeout=send_timeout)

    return unregister

class DiasporaLogConfig(LogConfig):
    """Academy LogConfig that ships logs to a Diaspora Kafka topic.

    Attach to the academy logger at DEBUG so all message-exchange
    context fields (academy.action, academy.src, academy.dest, etc.)
    are captured alongside user logs.

    The kafka_topic must already exist before the manager starts.
    The KafkaProducer is created inside init_logging() so the config
    object stays pickleable and can be sent to worker threads/processes.
    """

    def __init__(
        self,
        kafka_topic: str,
        logger_name: str = "academy",
        level: int = logging.DEBUG,
        send_timeout: int = 30,
        max_block_ms: int = 1000,
    ) -> None:
        super().__init__()
        self.kafka_topic = kafka_topic
        self.logger_name = logger_name
        self.level = level
        self.send_timeout = send_timeout
        self.max_block_ms = max_block_ms

    def init_logging(self) -> Callable[[], None]:
        from diaspora_event_sdk.sdk.kafka_client import KafkaProducer

        ssl_cafile = get_ssl_cafile()
        producer_kwargs: dict[str, Any] = {"max_block_ms": self.max_block_ms}
        if ssl_cafile:
            producer_kwargs["ssl_cafile"] = ssl_cafile
        producer = KafkaProducer(self.kafka_topic, **producer_kwargs)

        handler = DiasporaHandler(producer, self.kafka_topic, self.send_timeout)
        handler.setLevel(self.level)
        handler.setFormatter(
            logging.Formatter(DEFAULT_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"),
        )

        target = logging.getLogger(self.logger_name)
        target.setLevel(logging.DEBUG)
        target.addHandler(handler)

        def uninitialize() -> None:
            target.removeHandler(handler)
            # producer.close() sets _closed=True before calling flush, so
            # __del__ sees the flag and skips its own close attempt.
            with contextlib.suppress(Exception):
                producer.close(timeout=self.send_timeout)
            with contextlib.suppress(Exception):
                handler.close()

        return uninitialize


__all__ = [
    "DEFAULT_FORMAT",
    "DiasporaHandler",
    "DiasporaLogConfig",
    "get_ssl_cafile",
    "resolve_kafka_topic",
    "set_diaspora_logger",
]