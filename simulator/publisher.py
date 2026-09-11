import json
import random
import time
from datetime import datetime, timezone
from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = "project-streaming-telemetry"
TOPIC_ID = "telemetry-input-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

MACHINE_IDS = ["MCH-PLANT-01-001", "MCH-PLANT-01-002", "MCH-PLANT-02-005"]


def generate_telemetry():
    """Generates synthetic normal or anomalous machine telemetry records."""
    machine_id = random.choice(MACHINE_IDS)

    # Inject 15% probability of threshold breach anomaly
    is_anomaly = random.random() < 0.15

    if is_anomaly:
        temperature = round(random.uniform(80.5, 95.0), 2)
        vibration = round(random.uniform(5.1, 8.5), 2)
    else:
        temperature = round(random.uniform(60.0, 78.0), 2)
        vibration = round(random.uniform(1.0, 4.5), 2)

    payload = {
        "machine_id": machine_id,
        "temperature": temperature,
        "vibration": vibration,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return payload


def run_simulator(interval_seconds=2):
    print(f"Starting telemetry simulator publishing to {topic_path}...")
    try:
        while True:
            # Inject 5% probability of sending bad data payload to test DLQ
            if random.random() < 0.05:
                bad_payload = "{ malformed_json: true, missing_quotes }"
                publisher.publish(topic_path, bad_payload.encode("utf-8"))
                print(f"[DLQ TEST] Sent malformed payload.")
            else:
                data = generate_telemetry()
                message_bytes = json.dumps(data).encode("utf-8")
                future = publisher.publish(topic_path, message_bytes)
                print(f"[SENT] {data}")

            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\nSimulator stopped by user.")


if __name__ == "__main__":
    run_simulator()
