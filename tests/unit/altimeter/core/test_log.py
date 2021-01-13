import json
import logging
import re
import threading
from unittest.mock import patch

from altimeter.core.log import BaseLogEvent, EventName, BaseLogger


def escape_ansi(line):
    ansi_escape = re.compile(r"(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]")
    return ansi_escape.sub("", line)


class TestLogEvent(BaseLogEvent):
    TestEventA: EventName


def test_debug(caplog):
    caplog.set_level(logging.DEBUG)
    tid = threading.get_ident()
    with patch.dict("os.environ", {}, clear=True):
        logger = BaseLogger()
        logger.debug(event=TestLogEvent.TestEventA, a=1, b="hi-debug")
        record_dict = json.loads(caplog.records[0].message)
        expected_record_keys = {"event", "a", "b", "level", "tid", "timestamp"}
        assert set(record_dict.keys()) == expected_record_keys
        assert record_dict["a"] == 1
        assert record_dict["b"] == "hi-debug"
        assert record_dict["event"] == TestLogEvent.TestEventA.name
        assert record_dict["level"] == "debug"
        assert record_dict["tid"] == tid


def test_info(caplog):
    caplog.set_level(logging.DEBUG)
    tid = threading.get_ident()
    with patch.dict("os.environ", {}, clear=True):
        logger = BaseLogger()
        logger.info(event=TestLogEvent.TestEventA, a=1, b="hi-info")
        record_dict = json.loads(caplog.records[0].message)
        expected_record_keys = {"event", "a", "b", "level", "tid", "timestamp"}
        assert set(record_dict.keys()) == expected_record_keys
        assert record_dict["a"] == 1
        assert record_dict["b"] == "hi-info"
        assert record_dict["event"] == TestLogEvent.TestEventA.name
        assert record_dict["level"] == "info"
        assert record_dict["tid"] == tid


def test_warn(caplog):
    caplog.set_level(logging.DEBUG)
    tid = threading.get_ident()
    with patch.dict("os.environ", {}, clear=True):
        logger = BaseLogger()
        logger.warn(event=TestLogEvent.TestEventA, a=1, b="hi-warn")
        record_dict = json.loads(caplog.records[0].message)
        expected_record_keys = {"event", "a", "b", "level", "tid", "timestamp"}
        assert set(record_dict.keys()) == expected_record_keys
        assert record_dict["a"] == 1
        assert record_dict["b"] == "hi-warn"
        assert record_dict["event"] == TestLogEvent.TestEventA.name
        assert record_dict["level"] == "warning"
        assert record_dict["tid"] == tid


def test_warning(caplog):
    caplog.set_level(logging.DEBUG)
    tid = threading.get_ident()
    with patch.dict("os.environ", {}, clear=True):
        logger = BaseLogger()
        logger.warning(event=TestLogEvent.TestEventA, a=1, b="hi-warning")
        record_dict = json.loads(caplog.records[0].message)
        expected_record_keys = {"event", "a", "b", "level", "tid", "timestamp"}
        assert set(record_dict.keys()) == expected_record_keys
        assert record_dict["a"] == 1
        assert record_dict["b"] == "hi-warning"
        assert record_dict["event"] == TestLogEvent.TestEventA.name
        assert record_dict["level"] == "warning"
        assert record_dict["tid"] == tid


def test_error(caplog):
    caplog.set_level(logging.DEBUG)
    tid = threading.get_ident()
    with patch.dict("os.environ", {}, clear=True):
        logger = BaseLogger()
        logger.error(event=TestLogEvent.TestEventA, a=1, b="hi-error")
        record_dict = json.loads(caplog.records[0].message)
        expected_record_keys = {"event", "a", "b", "level", "tid", "timestamp"}
        assert set(record_dict.keys()) == expected_record_keys
        assert record_dict["a"] == 1
        assert record_dict["b"] == "hi-error"
        assert record_dict["event"] == TestLogEvent.TestEventA.name
        assert record_dict["level"] == "error"
        assert record_dict["tid"] == tid


def test_no_tid(caplog):
    caplog.set_level(logging.DEBUG)
    with patch.dict("os.environ", {}, clear=True):
        logger = BaseLogger(log_tid=False)
        logger.info(event=TestLogEvent.TestEventA, a=1, b="hi-info")
        record_dict = json.loads(caplog.records[0].message)
        expected_record_keys = {"event", "a", "b", "level", "timestamp"}
        assert set(record_dict.keys()) == expected_record_keys
        assert record_dict["a"] == 1
        assert record_dict["b"] == "hi-info"
        assert record_dict["event"] == TestLogEvent.TestEventA.name
        assert record_dict["level"] == "info"


def test_dev_log(caplog):
    caplog.set_level(logging.DEBUG)
    tid = threading.get_ident()
    with patch.dict("os.environ", {"DEV_LOG": "1"}, clear=True):
        logger = BaseLogger()
        logger.info(event=TestLogEvent.TestEventA, a=1, b="hi-info")
        decolorized = escape_ansi(caplog.records[0].message)
        fields = decolorized.split()
        assert "".join(fields[1:3]) == "[info]"
        assert fields[3] == TestLogEvent.TestEventA.name
        assert fields[4] == "a=1"
        assert fields[5] == "b=hi-info"
        assert fields[6] == f"tid={tid}"
