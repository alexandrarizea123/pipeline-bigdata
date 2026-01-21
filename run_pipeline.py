#!/usr/bin/env python3
"""
Pipeline orchestrator - runs all three pipeline components.
Usage: python run_pipeline.py
"""

import subprocess
import sys
import time
import os
import threading

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(SCRIPT_DIR, "scripts")

PRODUCER = os.path.join(SCRIPTS_DIR, "Sensor_prooducer.py")
PROCESSOR = os.path.join(SCRIPTS_DIR, "processing_data.py")
MONGO_SINK = os.path.join(SCRIPTS_DIR, "kafta_MongoAtlas.py")
API_SERVER = os.path.join(SCRIPTS_DIR, "api_server.py")

processes = []
stop_event = threading.Event()


def stream_output(name, proc):
    """Read and print output from a subprocess."""
    try:
        for line in iter(proc.stdout.readline, ''):
            if stop_event.is_set():
                break
            if line:
                print(f"[{name}] {line}", end="", flush=True)
    except Exception:
        pass


def cleanup():
    print("\n[Pipeline] Shutting down all components...")
    stop_event.set()
    for name, proc in processes:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            print(f"[Pipeline] Stopped {name}")


def main():
    print("=" * 60)
    print("IoT Data Pipeline - Starting all components")
    print("=" * 60)
    print("\nPrerequisites:")
    print("  - Docker containers running (docker-compose up -d in config/)")
    print("  - Python packages: kafka-python, pymongo, pandas, flask, flask-cors")
    print("\nPress Ctrl+C to stop all components\n")

    threads = []

    # Start API server first (for Grafana)
    print("[1/4] Starting API Server (api_server.py) on port 5000...")
    proc_api = subprocess.Popen(
        [sys.executable, API_SERVER],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    processes.append(("API", proc_api))
    t = threading.Thread(target=stream_output, args=("API", proc_api), daemon=True)
    t.start()
    threads.append(t)
    time.sleep(2)

    # Start processor (consumer) so it's ready when data arrives
    print("[2/4] Starting Stream Processor (processing_data.py)...")
    proc_processor = subprocess.Popen(
        [sys.executable, PROCESSOR],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    processes.append(("Processor", proc_processor))
    t = threading.Thread(target=stream_output, args=("Processor", proc_processor), daemon=True)
    t.start()
    threads.append(t)
    time.sleep(2)

    # Start MongoDB sink (consumer)
    print("[3/4] Starting MongoDB Sink (kafta_MongoAtlas.py)...")
    proc_sink = subprocess.Popen(
        [sys.executable, MONGO_SINK],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    processes.append(("MongoSink", proc_sink))
    t = threading.Thread(target=stream_output, args=("MongoSink", proc_sink), daemon=True)
    t.start()
    threads.append(t)
    time.sleep(2)

    # Start producer last (publishes data)
    print("[4/4] Starting Sensor Producer (Sensor_prooducer.py)...")
    proc_producer = subprocess.Popen(
        [sys.executable, PRODUCER],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    processes.append(("Producer", proc_producer))
    t = threading.Thread(target=stream_output, args=("Producer", proc_producer), daemon=True)
    t.start()
    threads.append(t)

    print("\n" + "=" * 60)
    print("All components started. Streaming output below...")
    print("=" * 60 + "\n")

    try:
        while True:
            # Check if all processes have finished
            all_done = all(proc.poll() is not None for _, proc in processes)
            if all_done:
                time.sleep(1)  # Let remaining output flush
                print("\n[Pipeline] All components finished.")
                break
            time.sleep(0.5)

    except KeyboardInterrupt:
        cleanup()
        sys.exit(0)

    cleanup()


if __name__ == "__main__":
    main()
