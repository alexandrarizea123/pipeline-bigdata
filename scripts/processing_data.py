import json
from kafka import KafkaConsumer, KafkaProducer

INPUT_TOPIC = "iot_sensors"
CLEANED_TOPIC = "iot_cleaned"
ALERTS_TOPIC = "iot_alerts"

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=1000 
)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

def is_valid(record):
    """Validate sensor limits"""
    try:
        return (
            -20 <= record["temp"] <= 60 and
            0 <= record["humidity"] <= 100 and
            record["co"] >= 0 and
            record["lpg"] >= 0 and
            record["smoke"] >= 0
        )
    except Exception:
        return False

def detect_alerts(record):
    alerts = []

    if record["smoke"] > 0.02:
        alerts.append("SMOKE_HIGH")
    if record["co"] > 0.01:
        alerts.append("CO_HIGH")
    if record["lpg"] > 0.01:
        alerts.append("LPG_HIGH")
    if record["motion"] and not record["light"]:
        alerts.append("SUS_MOTION")

    return alerts

def process_stream():
    print("Starting IoT stream processing...")

    for message in consumer:
        raw = message.value
        processed = {
            "timestamp": int(raw["ts"]),
            "device_id": raw["device"],
            "temp": float(raw["temp"]),
            "humidity": float(raw["humidity"]),
            "co": float(raw["co"]),
            "lpg": float(raw["lpg"]),
            "smoke": float(raw["smoke"]),
            "light": bool(raw["light"]),
            "motion": bool(raw["motion"])
        }

        if not is_valid(processed):
            print(f"Dropped invalid record from {processed['device_id']}")
            continue


        producer.send(CLEANED_TOPIC, processed)

        alerts = detect_alerts(processed)

        for alert_type in alerts:
            alert_event = {
                "timestamp": processed["timestamp"],
                "device_id": processed["device_id"],
                "alert_type": alert_type,
                "temp": processed["temp"],
                "smoke": processed["smoke"],
                "co": processed["co"],
                "lpg": processed["lpg"]
            }
            producer.send(ALERTS_TOPIC, alert_event)

        print(f"Processed {processed['device_id']} | Temp={processed['temp']} | Alerts={alerts}")

if __name__ == "__main__":
    try:
        process_stream()
    except KeyboardInterrupt:
        print("Stream processing stopped.")
