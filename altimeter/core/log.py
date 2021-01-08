"""Provides a class Logger with methods for logging."""
from contextlib import contextmanager
from dataclasses import dataclass
import logging
import os
import sys
import threading
from typing import cast, Any, Callable, Dict, Generator, List, MutableMapping, Tuple, Type, Union

import structlog
from structlog.stdlib import BoundLogger, get_logger as get_bound_logger


@dataclass(frozen=True)
class EventName:
    """Dataclass for log event names.

    Args:
        name: name of this event
    """

    name: str


class LogEventMeta(type):
    """Metaclass for LogEvents. This allows EventNames to specified in subclasses of BaseLogEvent
    as empty typed variables e.g.

        AuthToAccountStart: EventName

    Rather than requiring

        AuthToAccountStart: EventName("AuthToAccountStart")
    """

    def __new__(
        mcs, name: str, bases: Tuple[Type, ...], namespace: Dict[str, Any]
    ) -> "LogEventMeta":
        for annotation in namespace.get("__annotations__", []):
            namespace[annotation] = EventName(annotation)
        return cast(LogEventMeta, super().__new__(mcs, name, bases, namespace))


@dataclass(frozen=True)
class BaseLogEvent(metaclass=LogEventMeta):
    """Base class for LogEvent classes"""


class Singleton(type):
    """Singleton Metaclass"""

    _instances: Dict[Type[Any], Any] = {}

    def __call__(cls, *args: Any, **kwargs: Union[int, str, Dict]) -> Any:
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class BaseLogger:
    """Provides contextmanager 'bind' which can be use to bind
    keys to the logger using 'with' syntax, they will be removed from the logger
    in subsequent calls. In general use Logger, not BaseLogger directly."""

    def __init__(self, log_tid: bool = True, pretty_output: bool = False) -> None:
        self._log_tid = log_tid
        self.logger_stack = threading.local()
        self.pretty_output = pretty_output
        log_processors: List[
            Union[
                structlog.dev.ConsoleRenderer,
                structlog.processors.JSONRenderer,
                Callable[[logging.Logger, str, MutableMapping[str, Any]], MutableMapping[str, Any]],
            ]
        ] = [
            structlog.stdlib.add_log_level,
            structlog.stdlib.filter_by_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
        ]

        if os.environ.get("DEV_LOG", "0") == "1" or self.pretty_output:
            log_processors.append(structlog.dev.ConsoleRenderer(colors=True, force_colors=True))
        else:
            log_processors.append(structlog.processors.JSONRenderer(sort_keys=True))

        structlog.configure(
            logger_factory=structlog.stdlib.LoggerFactory(), processors=log_processors
        )
        """
        altimeter/core/log.py:83: error: Argument "processors" to "configure" has incompatible type
        "List[object]"; expected
        "Optional[Iterable[Callable[[Any, str, MutableMapping[str, Any]], Union[Mapping[str, Any], str, bytes, Tuple[Any, ...]]]]]"
        """

        logging.basicConfig(
            level=os.environ.get("LOG_LEVEL", "INFO"), stream=sys.stdout, format="%(message)s"
        )
        logging.getLogger("botocore").setLevel(logging.ERROR)

    def _get_loggers(self) -> List[BoundLogger]:
        if not hasattr(self.logger_stack, "loggers"):
            self.logger_stack.loggers = []
        return self.logger_stack.loggers

    def _get_current_logger(self) -> BoundLogger:
        loggers = self._get_loggers()
        if not loggers:
            logger = get_bound_logger()
            if self._log_tid:
                logger = logger.bind(tid=threading.get_ident())
            loggers.append(logger)
        return loggers[-1]

    def debug(self, event: EventName, **kwargs: Any) -> None:
        """Create DEBUG level log entry.

        Args:
            event: EventName object for this event
            kwargs: log event k/vs
        """
        self._get_current_logger().debug(event=event.name, **kwargs)

    def info(self, event: EventName, **kwargs: Any) -> None:
        """Create INFO level log entry.

        Args:
            event: EventName object for this event
            kwargs: log event k/vs
        """
        self._get_current_logger().info(event=event.name, **kwargs)

    def warn(self, event: EventName, **kwargs: Any) -> None:
        """Create WARN level log entry.

        Args:
            event: EventName object for this event
            kwargs: log event k/vs
        """
        self._get_current_logger().warn(event=event.name, **kwargs)

    def warning(self, event: EventName, **kwargs: Any) -> None:
        """Create WARN level log entry.

        Args:
            event: EventName object for this event
            kwargs: log event k/vs
        """
        self._get_current_logger().warning(event=event.name, **kwargs)

    def error(self, event: EventName, **kwargs: Any) -> None:
        """Create ERROR level log entry.

        Args:
            event: EventName object for this event
            kwargs: log event k/vs
        """
        self._get_current_logger().error(event=event.name, **kwargs)

    @contextmanager
    def bind(self, **bindings: Any) -> Generator[None, None, None]:
        """Context manager to bind a set of k/vs to the logger.  The k/vs will be removed
        when the with block exits."""
        new_logger = self._get_current_logger().bind(**bindings)
        loggers = self._get_loggers()
        loggers.append(new_logger)
        try:
            yield
        finally:
            loggers.pop()


class Logger(BaseLogger, metaclass=Singleton):
    """Singleton logger class"""

    pass
