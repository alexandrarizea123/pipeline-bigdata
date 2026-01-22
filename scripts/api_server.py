from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
from datetime import datetime, timezone
import os

app = Flask(__name__)
CORS(app)

# MongoDB Atlas connection
MONGO_URI = (
    "mongodb+srv://proiectibd_db_user:Fq20620LQNSFLUKY@cluster0.d0wtmr3.mongodb.net/"
    "iot?retryWrites=true&w=majority"
)
DB_NAME = "iot"

mongo = None
db = None


def get_db():
    global mongo, db
    if db is None:
        mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        db = mongo[DB_NAME]
    return db


@app.route('/')
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "service": "iot-api"})


@app.route('/api/processed', methods=['GET'])
def get_processed():
    """Get processed sensor data."""
    db = get_db()
    limit = int(request.args.get('limit', 100))

    docs = list(db.iot_processed.find(
        {},
        {'_id': 0}
    ).sort('ts', -1).limit(limit))

    # Convert datetime to ISO string
    for doc in docs:
        if 'ts' in doc and isinstance(doc['ts'], datetime):
            doc['ts'] = doc['ts'].isoformat()

    return jsonify(docs)


@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    """Get alert events."""
    db = get_db()
    limit = int(request.args.get('limit', 100))

    docs = list(db.iot_alerts.find(
        {},
        {'_id': 0}
    ).sort('ts', -1).limit(limit))

    # Convert datetime to ISO string
    for doc in docs:
        if 'ts' in doc and isinstance(doc['ts'], datetime):
            doc['ts'] = doc['ts'].isoformat()

    return jsonify(docs)


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get summary statistics."""
    db = get_db()

    processed_count = db.iot_processed.count_documents({})
    alerts_count = db.iot_alerts.count_documents({})

    # Get alert breakdown by type
    alert_pipeline = [
        {"$group": {"_id": "$alert_type", "count": {"$sum": 1}}}
    ]
    alert_breakdown = {doc['_id']: doc['count'] for doc in db.iot_alerts.aggregate(alert_pipeline)}

    # Get device count
    device_count = len(db.iot_processed.distinct('device_id'))

    # Get latest readings per device
    latest_pipeline = [
        {"$sort": {"ts": -1}},
        {"$group": {
            "_id": "$device_id",
            "latest_temp": {"$first": "$temp"},
            "latest_humidity": {"$first": "$humidity"},
            "latest_ts": {"$first": "$ts"}
        }}
    ]
    latest_readings = list(db.iot_processed.aggregate(latest_pipeline))

    for reading in latest_readings:
        if isinstance(reading.get('latest_ts'), datetime):
            reading['latest_ts'] = reading['latest_ts'].isoformat()

    return jsonify({
        "processed_count": processed_count,
        "alerts_count": alerts_count,
        "alert_breakdown": alert_breakdown,
        "device_count": device_count,
        "latest_readings": latest_readings
    })


@app.route('/api/timeseries/temperature', methods=['GET'])
def get_temperature_timeseries():
    """Get temperature time series data for Grafana."""
    db = get_db()
    limit = int(request.args.get('limit', 500))

    docs = list(db.iot_processed.find(
        {},
        {'ts': 1, 'temp': 1, 'device_id': 1, '_id': 0}
    ).sort('ts', -1).limit(limit))

    result = []
    for doc in docs:
        ts = doc.get('ts')
        if isinstance(ts, datetime):
            ts_iso = ts.isoformat() + "Z"
        elif isinstance(ts, (int, float)):
            if ts < 10_000_000_000:
                ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() + "Z"
            else:
                ts_iso = datetime.fromtimestamp(ts/1000, tz=timezone.utc).isoformat() + "Z"
        else:
            continue
        result.append({
            "timestamp": ts_iso,
            "temperature": doc.get('temp'),
            "device_id": doc.get('device_id')
        })

    return jsonify(result)


@app.route('/api/timeseries/humidity', methods=['GET'])
def get_humidity_timeseries():
    """Get humidity time series data for Grafana."""
    db = get_db()
    limit = int(request.args.get('limit', 500))

    docs = list(db.iot_processed.find(
        {},
        {'ts': 1, 'humidity': 1, 'device_id': 1, '_id': 0}
    ).sort('ts', -1).limit(limit))

    result = []
    for doc in docs:
        ts = doc.get('ts')
        if isinstance(ts, datetime):
            ts_iso = ts.isoformat() + "Z"
        elif isinstance(ts, (int, float)):
            if ts < 10_000_000_000:
                ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() + "Z"
            else:
                ts_iso = datetime.fromtimestamp(ts/1000, tz=timezone.utc).isoformat() + "Z"
        else:
            continue
        result.append({
            "timestamp": ts_iso,
            "humidity": doc.get('humidity'),
            "device_id": doc.get('device_id')
        })

    return jsonify(result)


if __name__ == '__main__':
    print("Starting IoT API Server...")
    print("Endpoints:")
    print("  GET /api/processed  - Sensor readings")
    print("  GET /api/alerts     - Alert events")
    print("  GET /api/stats      - Summary statistics")
    print("  GET /api/timeseries/temperature - Temperature time series")
    print("  GET /api/timeseries/humidity    - Humidity time series")
    print("")
    app.run(host='0.0.0.0', port=5000, debug=False)
