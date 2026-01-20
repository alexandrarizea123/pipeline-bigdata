import json
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING, errors

# Config
BOOTSTRAP = "localhost:9092"  # Kafka (Docker)
TOPICS = ["iot_processed", "iot_alerts"]

# MongoDB Atlas (cu /iot)
MONGO_URI = (
    "mongodb+srv://proiectibd_db_user:Fq20620LQNSFLUKY@cluster0.d0wtmr3.mongodb.net/"
    "iot?retryWrites=true&w=majority"
)
DB_NAME = "iot"

BATCH_SIZE = 1000
POLL_TIMEOUT_MS = 1000
IDLE_EXIT_SECONDS = 20  # daca nu mai vin mesaje, iese 


def add_ts_field(doc: dict) -> dict:
    """
    Pentru Grafana e util un camp de tip Date.
    Daca documentul are 'timestamp' numeric (sec sau ms) si nu are deja 'ts',
    adaugam doc['ts'] = datetime(UTC).
    """
    if "ts" in doc:
        return doc

    ts = doc.get("timestamp")
    if isinstance(ts, (int, float)):
        # Heuristica: valori foarte mari sunt de obicei in milisecunde
        if ts > 10_000_000_000:
            dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        doc["ts"] = dt

    return doc


def ensure_indexes(db):
    # iot_processed: timp + (device,timp)
    try:
        db.iot_processed.create_index([("ts", ASCENDING)], name="idx_ts")
    except Exception:
        pass
    try:
        db.iot_processed.create_index([("device_id", ASCENDING), ("ts", ASCENDING)], name="idx_device_ts")
    except Exception:
        pass

    # iot_alerts: timp + (device,timp) + (tip_alerte,timp) daca exista
    try:
        db.iot_alerts.create_index([("ts", ASCENDING)], name="idx_ts")
    except Exception:
        pass
    try:
        db.iot_alerts.create_index([("device_id", ASCENDING), ("ts", ASCENDING)], name="idx_device_ts")
    except Exception:
        pass

    # campul de tip alerta poate fi diferit in functie de script (alert_type / alert / type)
    for field in ["alert_type", "alert", "type"]:
        try:
            db.iot_alerts.create_index([(field, ASCENDING), ("ts", ASCENDING)], name=f"idx_{field}_ts")
            break
        except Exception:
            continue


def main():
    # Conectare la Atlas
    mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    try:
        mongo.admin.command("ping")
    except errors.ServerSelectionTimeoutError as e:
        print("Nu pot conecta la MongoDB Atlas.")
        print("Verifica: Network Access (IP), user/parola, si ca ai internet.")
        print(e)
        return

    db = mongo[DB_NAME]
    ensure_indexes(db)

    # Consumatori Kafka (citim de la inceput ca sa importam tot)
    consumers = {}
    for topic in TOPICS:
        consumers[topic] = KafkaConsumer(
            topic,
            bootstrap_servers=[BOOTSTRAP],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=f"atlas-sink-{topic}",  # grup separat
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

    print("✅ Kafka -> MongoDB Atlas import pornit")
    print(f"   DB: {DB_NAME} | topics: {', '.join(TOPICS)}")
    print("   (se opreste singur dupa ce nu mai vin mesaje)")
    print("   Ctrl+C pentru stop manual.\n")

    last_any_msg = time.time()

    try:
        while True:
            got_any = False

            for topic, consumer in consumers.items():
                records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=BATCH_SIZE)
                if not records:
                    continue

                batch = []
                for _tp, msgs in records.items():
                    for m in msgs:
                        if isinstance(m.value, dict):
                            batch.append(add_ts_field(m.value))

                if batch:
                    got_any = True
                    last_any_msg = time.time()

                    col = db[topic]  # aceeasi denumire ca topicul
                    try:
                        col.insert_many(batch, ordered=False)
                        print(f"Inserted {len(batch)} docs -> {DB_NAME}.{topic}")
                    except Exception as e:
                        # daca apare ceva sporadic, nu oprim importul
                        print(f"[WARN] Insert issue on {topic}: {e}")

            # daca nu mai vin mesaje un timp, iesim
            if not got_any and (time.time() - last_any_msg) > IDLE_EXIT_SECONDS:
                print("\n✅ Import terminat (nu mai sunt mesaje noi).")
                break

    except KeyboardInterrupt:
        print("\nOprit manual.")
    finally:
        for c in consumers.values():
            try:
                c.close()
            except Exception:
                pass
        mongo.close()


if __name__ == "__main__":
    main()
