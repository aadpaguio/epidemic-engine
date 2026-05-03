"""Unit tests for alert/alert_service.py and alert/notifier.py.

These tests do not require Postgres or Slack; database calls go through
``FakeConn`` which records SQL strings and returns scripted rows, and the
Slack notifier swaps in a fake URL opener so we can assert request bodies
without a network call.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch

import pytest

from alert import alert_service
from alert.notifier import SlackNotifier, build_slack_message


# ---------------------------------------------------------------------------
# Fake psycopg2 connection
# ---------------------------------------------------------------------------


class FakeCursor:
    def __init__(self, conn: "FakeConn") -> None:
        self._conn = conn
        self._row: Any = None
        self._rows: list[Any] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] | None = None) -> None:
        self._conn.queries.append((sql, params))
        normalized = re.sub(r"\s+", " ", sql).strip().lower()
        result = self._conn.match_response(normalized, params)
        if isinstance(result, list):
            self._rows = list(result)
            self._row = self._rows[0] if self._rows else None
        else:
            self._row = result
            self._rows = [result] if result is not None else []

    def fetchone(self) -> Any:
        return self._row

    def fetchall(self) -> list[Any]:
        return list(self._rows)


class FakeConn:
    """Minimal psycopg2-style connection that returns scripted query results."""

    def __init__(self) -> None:
        self.queries: list[tuple[str, Any]] = []
        self.commits = 0
        self.rollbacks = 0
        # responses is a list of (predicate, value) pairs evaluated in order.
        self.responses: list[tuple[Any, Any]] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None

    def add_response(self, predicate, value) -> None:
        """``predicate`` may be a substring or a callable (sql, params) -> bool."""
        self.responses.append((predicate, value))

    def match_response(self, sql_lower: str, params) -> Any:
        for predicate, value in self.responses:
            if callable(predicate):
                if predicate(sql_lower, params):
                    return value
            elif isinstance(predicate, str):
                if predicate in sql_lower:
                    return value
        return None


# ---------------------------------------------------------------------------
# load_rules
# ---------------------------------------------------------------------------


def test_load_rules_uses_provided_path(tmp_path):
    rules_path = tmp_path / "thresholds.json"
    rules_path.write_text(
        json.dumps(
            {
                "demo": {
                    "metric": "event_count",
                    "windows": [{"name": "5m", "minutes": 5, "threshold": 10}],
                    "severity": "warning",
                }
            }
        )
    )
    rules = alert_service.load_rules(str(rules_path))
    assert "demo" in rules
    assert rules["demo"]["metric"] == "event_count"


def test_load_rules_falls_back_to_defaults(tmp_path):
    missing = tmp_path / "does-not-exist.json"
    rules = alert_service.load_rules(str(missing))
    # Repo-tree fallback (alert/thresholds.json) should populate the rules.
    assert "high_event_volume" in rules or "high_avg_severity" in rules


def test_load_rules_built_in_defaults_when_no_file_found(monkeypatch, tmp_path):
    # Force both candidate paths to misses so we exercise DEFAULT_METRIC_RULES.
    monkeypatch.setattr(
        alert_service,
        "_candidate_rule_paths",
        lambda path: [tmp_path / "missing-1.json", tmp_path / "missing-2.json"],
    )
    rules = alert_service.load_rules("ignored")
    assert rules.keys() == alert_service.DEFAULT_METRIC_RULES.keys()


# ---------------------------------------------------------------------------
# evaluate_metric_rule
# ---------------------------------------------------------------------------


def test_evaluate_metric_rule_fires_when_min_breaching_met():
    rule = {
        "metric": "event_count",
        "severity": "warning",
        "min_windows_breaching": 2,
    }
    measurements = [
        {"window": "5m", "value": 100, "threshold": 50, "breach": True},
        {"window": "1h", "value": 800, "threshold": 1000, "breach": False},
        {"window": "1d", "value": 30000, "threshold": 20000, "breach": True},
    ]
    result = alert_service.evaluate_metric_rule("demo", rule, measurements)
    assert result is not None
    assert result["n_breaches"] == 2
    assert result["severity"] == "warning"


def test_evaluate_metric_rule_suppresses_below_min():
    rule = {
        "metric": "event_count",
        "severity": "warning",
        "min_windows_breaching": 2,
    }
    measurements = [
        {"window": "5m", "value": 100, "threshold": 50, "breach": True},
        {"window": "1h", "value": 800, "threshold": 1000, "breach": False},
    ]
    assert alert_service.evaluate_metric_rule("demo", rule, measurements) is None


def test_evaluate_metric_rule_handles_no_measurements():
    assert alert_service.evaluate_metric_rule("demo", {"metric": "event_count"}, []) is None


# ---------------------------------------------------------------------------
# query helpers
# ---------------------------------------------------------------------------


def test_query_event_count_uses_minutes_param():
    conn = FakeConn()
    conn.add_response("from raw_typed_events", (123,))
    n = alert_service.query_event_count(conn, 5)
    assert n == 123
    sql, params = conn.queries[-1]
    assert params == (5,)
    assert "raw_typed_events" in sql.lower()


def test_query_avg_severity_returns_float():
    conn = FakeConn()
    conn.add_response("avg(numeric_severity)", (2.5,))
    v = alert_service.query_avg_severity(conn, 60)
    assert v == pytest.approx(2.5)


def test_query_avg_severity_returns_none_when_no_rows():
    conn = FakeConn()
    conn.add_response("avg(numeric_severity)", (None,))
    assert alert_service.query_avg_severity(conn, 60) is None


# ---------------------------------------------------------------------------
# emit_alert + cooldown + escalation
# ---------------------------------------------------------------------------


class RecordingNotifier:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    def send_alert(self, **kwargs: Any) -> bool:
        self.sent.append(kwargs)
        return True


def test_emit_alert_persists_logs_and_notifies(monkeypatch, capsys):
    conn = FakeConn()

    # No previous row → cooldown allowed → upsert + log + slack.
    conn.add_response(
        lambda s, p: "select created_at, updated_at, status" in s,
        None,
    )
    conn.add_response(
        lambda s, p: "select updated_at, status from alert_events" in s,
        None,
    )
    conn.add_response(lambda s, p: "insert into alert_events" in s, None)

    notifier = RecordingNotifier()
    monkeypatch.setattr(alert_service, "SLACK_NOTIFY_SEVERITIES", ("critical", "warning"))

    fired = alert_service.emit_alert(
        conn,
        notifier,
        alert_key="metric:high_event_volume",
        alert_type="event_count",
        severity="warning",
        message="too many events",
        raw_payload={"detail": "x"},
    )

    assert fired is True
    assert len(notifier.sent) == 1
    assert notifier.sent[0]["severity"] == "warning"
    assert notifier.sent[0]["escalated"] is False

    out = capsys.readouterr().out
    assert "[ALERT]" in out
    body = json.loads(out.split("[ALERT] ", 1)[1].splitlines()[0])
    assert body["alert_key"] == "metric:high_event_volume"
    assert body["severity"] == "warning"


def test_emit_alert_suppressed_by_cooldown(monkeypatch):
    conn = FakeConn()
    now = datetime.now(timezone.utc)

    conn.add_response(
        lambda s, p: "select created_at, updated_at, status" in s,
        (now, now - timedelta(seconds=30), "active"),
    )
    conn.add_response(
        lambda s, p: "select updated_at, status from alert_events" in s,
        (now - timedelta(seconds=30), "active"),
    )

    notifier = RecordingNotifier()
    fired = alert_service.emit_alert(
        conn,
        notifier,
        alert_key="metric:high_event_volume",
        alert_type="event_count",
        severity="warning",
        message="too many events",
        raw_payload={},
        cooldown_seconds=300,
    )
    assert fired is False
    assert notifier.sent == []


def test_emit_alert_marks_escalated_after_threshold(monkeypatch, capsys):
    conn = FakeConn()
    long_ago = datetime.now(timezone.utc) - timedelta(seconds=alert_service.ESCALATE_AFTER_SECONDS + 60)

    conn.add_response(
        lambda s, p: "select created_at, updated_at, status" in s,
        (long_ago, long_ago, "active"),
    )
    conn.add_response(
        lambda s, p: "select updated_at, status from alert_events" in s,
        # Fresh enough for cooldown to NOT block
        (long_ago, "active"),
    )
    conn.add_response(lambda s, p: "insert into alert_events" in s, None)

    notifier = RecordingNotifier()
    monkeypatch.setattr(alert_service, "SLACK_NOTIFY_SEVERITIES", ("critical", "warning"))
    alert_service.emit_alert(
        conn,
        notifier,
        alert_key="pipeline:stale_predictions",
        alert_type="prediction_freshness",
        severity="critical",
        message="stale!",
        raw_payload={},
        cooldown_seconds=0,
    )
    assert notifier.sent and notifier.sent[0]["escalated"] is True

    body = json.loads(capsys.readouterr().out.split("[ALERT] ", 1)[1].splitlines()[0])
    assert body["escalated"] is True
    assert "escalation_active_seconds" in body


def test_emit_alert_skips_slack_when_severity_not_subscribed(monkeypatch):
    conn = FakeConn()
    conn.add_response(lambda s, p: "select created_at, updated_at, status" in s, None)
    conn.add_response(lambda s, p: "select updated_at, status from alert_events" in s, None)
    conn.add_response(lambda s, p: "insert into alert_events" in s, None)

    notifier = RecordingNotifier()
    monkeypatch.setattr(alert_service, "SLACK_NOTIFY_SEVERITIES", ("critical",))
    fired = alert_service.emit_alert(
        conn,
        notifier,
        alert_key="metric:noisy",
        alert_type="event_count",
        severity="info",
        message="info",
        raw_payload={},
    )
    assert fired is True
    assert notifier.sent == []


# ---------------------------------------------------------------------------
# Slack message rendering and notifier transport
# ---------------------------------------------------------------------------


def test_build_slack_message_includes_severity_and_mention():
    text = build_slack_message(
        alert_key="metric:high_event_volume",
        alert_type="event_count",
        severity="critical",
        message="rate spiked",
        payload={
            "region": "Boston",
            "measurements": [
                {"window": "5m", "value": 999, "threshold": 50, "breach": True},
            ],
        },
        critical_mention="<!channel>",
    )
    assert "*[CRITICAL]*" in text
    assert "<!channel>" in text
    assert "metric:high_event_volume" in text
    assert "5m: value=999 threshold=50" in text


def test_build_slack_message_marks_escalation():
    text = build_slack_message(
        alert_key="pipeline:stale_predictions",
        alert_type="prediction_freshness",
        severity="warning",
        message="hello",
        payload={"escalation_active_seconds": 7300.0},
        escalated=True,
    )
    assert "ESCALATED" in text
    assert "active for" in text


class FakeOpener:
    def __init__(self, *, status: int = 200, raise_exc: Exception | None = None):
        self.status = status
        self.raise_exc = raise_exc
        self.calls: list[tuple[str, bytes]] = []

    def urlopen(self, req, timeout=None):
        self.calls.append((req.full_url, req.data))
        if self.raise_exc is not None:
            raise self.raise_exc
        return _FakeResp(self.status)


class _FakeResp:
    def __init__(self, status: int):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


def test_slack_notifier_sends_text_payload():
    opener = FakeOpener(status=200)
    n = SlackNotifier("https://hooks.slack/test", opener=opener)
    assert n.send_text("hello world") is True
    url, data = opener.calls[-1]
    assert url == "https://hooks.slack/test"
    assert json.loads(data.decode())["text"] == "hello world"


def test_slack_notifier_swallows_network_errors():
    import urllib.error

    opener = FakeOpener(raise_exc=urllib.error.URLError("boom"))
    n = SlackNotifier("https://hooks.slack/test", opener=opener)
    assert n.send_alert(
        alert_key="x",
        alert_type="t",
        severity="warning",
        message="m",
        payload={},
    ) is False


def test_slack_notifier_requires_webhook_url():
    with pytest.raises(ValueError):
        SlackNotifier("")


# ---------------------------------------------------------------------------
# Main CLI smoke modes
# ---------------------------------------------------------------------------


def test_test_slack_cli_returns_zero_when_send_succeeds(monkeypatch, capsys):
    fake = type(
        "F",
        (),
        {
            "send_test_message": staticmethod(lambda: True),
        },
    )()

    monkeypatch.setattr(alert_service, "build_notifier", lambda: fake)
    rc = alert_service.main(["--test-slack"])
    assert rc == 0
    assert "Slack test message sent" in capsys.readouterr().out


def test_test_slack_cli_returns_one_when_disabled(monkeypatch, capsys):
    monkeypatch.setattr(alert_service, "build_notifier", lambda: None)
    rc = alert_service.main(["--test-slack"])
    assert rc == 1
    assert "Slack is not configured" in capsys.readouterr().err


def test_main_once_runs_a_single_cycle(monkeypatch):
    monkeypatch.setattr(alert_service, "DATABASE_URL", "postgresql://x:y@h:5432/d")
    monkeypatch.setattr(alert_service, "build_notifier", lambda: None)
    monkeypatch.setattr(alert_service, "load_rules", lambda: {})

    cycles: list[int] = []

    class _StubConn:
        def close(self) -> None:
            return None

    def fake_connect(_dsn):
        return _StubConn()

    def fake_run_cycle(_conn, _notifier, _rules):
        cycles.append(1)

    monkeypatch.setattr(alert_service.psycopg2, "connect", fake_connect)
    monkeypatch.setattr(alert_service, "run_cycle", fake_run_cycle)

    # Patch time.sleep so a stuck loop would be a test failure, not a hang.
    sleeps: list[float] = []
    monkeypatch.setattr(alert_service.time, "sleep", lambda s: sleeps.append(s))

    rc = alert_service.main(["--once"])
    assert rc == 0
    assert cycles == [1]
    assert sleeps == []  # --once must not sleep
