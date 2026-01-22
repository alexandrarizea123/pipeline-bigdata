import csv
import random
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "..", "data", "iot_demo_data.csv")

DEVICES = [
    "b8:27:eb:bf:9d:51",
    "00:0f:00:70:91:0a",
    "1c:bf:ce:15:ec:4d",
    "b8:27:eb:a6:d4:12",
]

def generate_record(timestamp):
    device = random.choice(DEVICES)

    # Normal sensor readings
    temp = round(random.uniform(18.0, 30.0), 1)
    humidity = round(random.uniform(40.0, 80.0), 1)
    co = round(random.uniform(0.001, 0.008), 6)
    lpg = round(random.uniform(0.003, 0.009), 6)
    smoke = round(random.uniform(0.005, 0.015), 6)
    light = random.choice([True, True, True, False])  # Usually has light
    motion = random.choice([False, False, False, True])  # Occasional motion

    # 5% chance of anomaly
    if random.random() < 0.05:
        anomaly = random.choice(["smoke", "co", "lpg", "sus_motion"])
        if anomaly == "smoke":
            smoke = round(random.uniform(0.025, 0.05), 6)
        elif anomaly == "co":
            co = round(random.uniform(0.015, 0.03), 6)
        elif anomaly == "lpg":
            lpg = round(random.uniform(0.015, 0.025), 6)
        elif anomaly == "sus_motion":
            motion = True
            light = False

    return {
        "ts": str(timestamp),
        "device": device,
        "co": str(co),
        "humidity": str(humidity),
        "light": str(light).lower(),
        "lpg": str(lpg),
        "motion": str(motion).lower(),
        "smoke": str(smoke),
        "temp": str(temp),
    }


def main():
    num_records = int(sys.argv[1]) if len(sys.argv) > 1 else 1000

    # Start timestamp (recent)
    base_ts = 1700000000.0  # Nov 2023

    print(f"Generating {num_records} synthetic IoT records...")

    with open(OUTPUT_FILE, "w", newline="") as f:
        fieldnames = ["ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp"]
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()

        for i in range(num_records):
            timestamp = base_ts + (i * 3.5)  # ~3.5 seconds between readings
            record = generate_record(timestamp)
            writer.writerow(record)

    print(f"Created: {OUTPUT_FILE}")
    print(f"Records: {num_records}")


if __name__ == "__main__":
    main()
