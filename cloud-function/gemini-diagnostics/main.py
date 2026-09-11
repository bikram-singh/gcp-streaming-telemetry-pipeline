import base64
import hashlib
import json
import os
import logging
import functions_framework
from google import genai
from google.cloud import bigquery

# Configure Logging
logging.basicConfig(level=logging.INFO)

# Environment Variables & Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT", "project-streaming-telemetry")
DATASET_ID = os.environ.get("BIGQUERY_DATASET", "telemetry_analytics")
TABLE_ID = os.environ.get("BIGQUERY_TABLE", "incident_log")
LOCATION = os.environ.get("GCP_LOCATION", "us-central1")

# Initialize Vertex AI-backed Gemini Client and BigQuery Client
gemini_client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
bq_client = bigquery.Client(project=PROJECT_ID)


@functions_framework.cloud_event
def analyze_telemetry_alert(cloud_event):
    """Triggered by a Cloud Pub/Sub message on threshold breach alert."""
    try:
        # Decode Eventarc Pub/Sub payload
        pubsub_message = cloud_event.data.get("message", {})
        if not pubsub_message or "data" not in pubsub_message:
            logging.error("Received empty or malformed Pub/Sub payload.")
            return

        raw_data = base64.b64decode(pubsub_message["data"]).decode("utf-8")
        alert_data = json.loads(raw_data)

        machine_id = alert_data.get("machine_id")
        avg_temp = alert_data.get("avg_temperature")
        avg_vib = alert_data.get("avg_vibration")
        window_end = alert_data.get("window_end")

        logging.info(f"Processing incident breach for Machine ID: {machine_id} at {window_end}")

        # Deterministic Idempotency Key (Hash of machine_id + window_end)
        raw_key = f"{machine_id}_{window_end}"
        incident_id = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:16]

        # Construct Root-Cause Diagnostic Prompt for Gemini 2.5 Flash
        prompt = (
            f"You are an industrial reliability engineer. "
            f"Anomalous telemetry detected on Machine {machine_id}:\n"
            f"- 1-Minute Avg Temperature: {avg_temp}°C (Threshold: 80.0°C)\n"
            f"- 1-Minute Avg Vibration: {avg_vib} mm/s (Threshold: 5.0 mm/s)\n"
            f"- Window End Timestamp: {window_end}\n\n"
            f"Provide a concise, 2-sentence root-cause diagnostic and immediate action required."
        )

        # Call Gemini 2.5 Flash via Vertex AI API
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        diagnostic_text = response.text.strip() if response.text else "No diagnostic output generated."

        # Insert Incident Log into BigQuery.
        # row_ids uses incident_id as the dedup key so a redelivered Eventarc
        # message (same machine_id + window_end) does not create a duplicate
        # row within BigQuery's streaming-insert dedup window.
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        row_to_insert = [
            {
                "incident_id": incident_id,
                "machine_id": machine_id,
                "avg_temperature": float(avg_temp),
                "avg_vibration": float(avg_vib),
                "diagnostic": diagnostic_text,
                "detected_at": window_end
            }
        ]

        errors = bq_client.insert_rows_json(
            table_ref,
            row_to_insert,
            row_ids=[incident_id]
        )
        if errors:
            logging.error(f"Failed to insert incident record to BigQuery: {errors}")
        else:
            logging.info(f"Successfully logged incident {incident_id} into BigQuery.")

    except Exception as e:
        # Logged and swallowed rather than re-raised: a transient failure here
        # is dropped, not retried by Eventarc. Deliberate lab trade-off —
        # revisit if at-least-once processing is required (raise instead,
        # deploy with --retry, and keep row_ids in place so retries stay safe).
        logging.exception(f"Unhandled error processing alert payload: {e}")
