import base64
import json
from unittest.mock import MagicMock

import main as function_module


def _make_cloud_event(payload: dict):
    """Builds a fake Eventarc CloudEvent matching the shape main.py expects:
    cloud_event.data['message']['data'] is base64-encoded Pub/Sub message bytes."""
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    event = MagicMock()
    event.data = {"message": {"data": encoded}}
    return event


def _reset_mocks():
    function_module.gemini_client.reset_mock()
    function_module.bq_client.reset_mock()


def test_successful_incident_is_logged_to_bigquery():
    _reset_mocks()
    mock_response = MagicMock()
    mock_response.text = "Test diagnostic."
    function_module.gemini_client.models.generate_content.return_value = mock_response
    function_module.bq_client.insert_rows_json.return_value = []  # no errors

    event = _make_cloud_event({
        "machine_id": "MCH-01",
        "avg_temperature": 91.0,
        "avg_vibration": 6.0,
        "window_end": "2026-01-01T00:01:00Z",
    })

    function_module.analyze_telemetry_alert(event)

    function_module.gemini_client.models.generate_content.assert_called_once()
    function_module.bq_client.insert_rows_json.assert_called_once()

    args, kwargs = function_module.bq_client.insert_rows_json.call_args
    inserted_row = args[1][0]
    assert inserted_row["machine_id"] == "MCH-01"
    assert inserted_row["avg_temperature"] == 91.0
    assert inserted_row["diagnostic"] == "Test diagnostic."
    # Idempotency key must actually be passed as row_ids, not just computed
    assert kwargs["row_ids"] == [inserted_row["incident_id"]]


def test_same_machine_and_window_produce_same_incident_id():
    # Idempotency key is a deterministic hash of machine_id + window_end —
    # pin that two identical alerts (e.g. a retried delivery) produce the
    # same incident_id, which is what makes row_ids-based dedup work at all.
    _reset_mocks()
    mock_response = MagicMock()
    mock_response.text = "Diagnostic."
    function_module.gemini_client.models.generate_content.return_value = mock_response
    function_module.bq_client.insert_rows_json.return_value = []

    payload = {
        "machine_id": "MCH-01",
        "avg_temperature": 91.0,
        "avg_vibration": 6.0,
        "window_end": "2026-01-01T00:01:00Z",
    }

    function_module.analyze_telemetry_alert(_make_cloud_event(payload))
    first_id = function_module.bq_client.insert_rows_json.call_args[0][1][0]["incident_id"]

    function_module.analyze_telemetry_alert(_make_cloud_event(payload))
    second_id = function_module.bq_client.insert_rows_json.call_args[0][1][0]["incident_id"]

    assert first_id == second_id


def test_malformed_payload_does_not_call_gemini_or_bigquery():
    _reset_mocks()
    event = MagicMock()
    event.data = {"message": {"data": base64.b64encode(b"not valid json").decode("utf-8")}}

    function_module.analyze_telemetry_alert(event)

    function_module.gemini_client.models.generate_content.assert_not_called()
    function_module.bq_client.insert_rows_json.assert_not_called()


def test_empty_pubsub_payload_does_not_raise():
    _reset_mocks()
    event = MagicMock()
    event.data = {}

    # Should return quietly, not throw — this path is hit if Eventarc ever
    # delivers a malformed envelope.
    function_module.analyze_telemetry_alert(event)

    function_module.gemini_client.models.generate_content.assert_not_called()


def test_bigquery_insert_error_is_logged_not_raised():
    # bq_client.insert_rows_json returning errors should not crash the
    # function — it's swallowed and logged (see main.py's design note on
    # at-most-once processing).
    _reset_mocks()
    mock_response = MagicMock()
    mock_response.text = "Diagnostic."
    function_module.gemini_client.models.generate_content.return_value = mock_response
    function_module.bq_client.insert_rows_json.return_value = [{"error": "insert failed"}]

    event = _make_cloud_event({
        "machine_id": "MCH-01",
        "avg_temperature": 91.0,
        "avg_vibration": 6.0,
        "window_end": "2026-01-01T00:01:00Z",
    })

    # Should not raise
    function_module.analyze_telemetry_alert(event)
