"""Slack webhook notifier for epidemic-alert-service.

Uses only the Python standard library so the alert image stays minimal.
The notifier is intentionally fail-soft: any HTTP/transport error is logged
and surfaced as a return value, and the alert service keeps running.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


SEVERITY_EMOJI = {
    "critical": ":rotating_light:",
    "warning": ":warning:",
    "info": ":information_source:",
}


def build_slack_message(
    *,
    alert_key: str,
    alert_type: str,
    severity: str,
    message: str,
    payload: dict[str, Any],
    channel_name: str = "#epidemic-alerts",
    critical_mention: str = "<!channel>",
    escalated: bool = False,
) -> str:
    """Build the Slack `text` field for a single alert notification.

    Pure function so unit tests can assert the exact rendering without a
    network call. Channel name is included only as a human-readable hint;
    Slack itself uses whatever channel the incoming webhook was provisioned
    for.
    """
    emoji = SEVERITY_EMOJI.get(severity, ":bell:")
    prefix_parts: list[str] = [emoji, f"*[{severity.upper()}]*"]
    if escalated:
        prefix_parts.append(":arrow_double_up: *ESCALATED*")
    if severity == "critical" and critical_mention:
        prefix_parts.append(critical_mention)
    prefix = " ".join(prefix_parts)

    region = payload.get("region") if isinstance(payload, dict) else None
    bucket_start = payload.get("bucket_start") if isinstance(payload, dict) else None
    run_id = payload.get("run_id") if isinstance(payload, dict) else None

    lines: list[str] = [
        f"{prefix} `{alert_type}` :: {channel_name}",
        f"> {message}",
        f"_alert_key:_ `{alert_key}`",
    ]
    if region:
        lines.append(f"_region:_ `{region}`")
    if bucket_start:
        lines.append(f"_bucket_start:_ `{bucket_start}`")
    if run_id:
        lines.append(f"_run_id:_ `{run_id}`")

    measurements = payload.get("measurements") if isinstance(payload, dict) else None
    if measurements:
        lines.append("_windows:_")
        for m in measurements:
            mark = ":x:" if m.get("breach") else ":white_check_mark:"
            value = m.get("value")
            threshold = m.get("threshold")
            window_name = m.get("window")
            lines.append(
                f"  {mark} {window_name}: value={value} threshold={threshold}"
            )

    if escalated:
        active_seconds = payload.get("escalation_active_seconds")
        if active_seconds is not None:
            try:
                hours = float(active_seconds) / 3600.0
                lines.append(f"_active for:_ {hours:.1f}h")
            except (TypeError, ValueError):
                pass

    return "\n".join(lines)


class SlackNotifier:
    """Minimal Slack incoming-webhook client."""

    def __init__(
        self,
        webhook_url: str,
        *,
        channel_name: str = "#epidemic-alerts",
        critical_mention: str = "<!channel>",
        timeout_seconds: int = 5,
        opener: Any | None = None,
    ) -> None:
        if not webhook_url:
            raise ValueError("webhook_url is required for SlackNotifier")
        self.webhook_url = webhook_url
        self.channel_name = channel_name
        self.critical_mention = critical_mention
        self.timeout_seconds = max(1, int(timeout_seconds))
        self._opener = opener

    def _post(self, body: dict[str, Any]) -> bool:
        encoded = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=encoded,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            opener = self._opener or urllib.request
            with opener.urlopen(req, timeout=self.timeout_seconds) as resp:
                status = getattr(resp, "status", 200)
                ok = 200 <= status < 300
                if not ok:
                    logger.warning(
                        "slack webhook returned non-2xx: status=%s", status
                    )
                return ok
        except (urllib.error.URLError, OSError, ValueError) as exc:
            logger.warning("slack webhook send failed: %s", exc)
            return False

    def send_text(self, text: str) -> bool:
        return self._post({"text": text})

    def send_alert(
        self,
        *,
        alert_key: str,
        alert_type: str,
        severity: str,
        message: str,
        payload: dict[str, Any],
        escalated: bool = False,
    ) -> bool:
        text = build_slack_message(
            alert_key=alert_key,
            alert_type=alert_type,
            severity=severity,
            message=message,
            payload=payload,
            channel_name=self.channel_name,
            critical_mention=self.critical_mention,
            escalated=escalated,
        )
        return self.send_text(text)

    def send_test_message(self) -> bool:
        text = (
            ":white_check_mark: epidemic-alert-service Slack delivery test "
            f"({self.channel_name}) — alerting is configured."
        )
        return self.send_text(text)
