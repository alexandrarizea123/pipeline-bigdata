import time
import json
import pandas as pd
from kafka import KafkaProducer

# Configurare Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8')  # Cheie pentru scalabilitate
)

TOPIC_NAME = 'iot_sensors'
CSV_FILE_PATH = r"D:\Facultate\1.MASTER\Anul_1\M1_SEM1\Big Data\Proiect\Date\Environmental Sensor Telemetry Data\iot_telemetry_data2.csv"

def run_simulation():
    print(f"Începem simularea citirii datelor din {CSV_FILE_PATH}...")
    
    
    for chunk in pd.read_csv(CSV_FILE_PATH, sep=';', header=1, chunksize=1000):
        
        for index, row in chunk.iterrows():
            
            sensor_data = row.to_dict()
            
            
            device_id = str(sensor_data['device'])
            
           
            producer.send(TOPIC_NAME, key=device_id, value=sensor_data)
            
            print(f"Trimis: {device_id} -> {sensor_data['ts']}")
            
            
            time.sleep(0.01)
            
        # Golim buffer-ul periodic
        producer.flush()

if __name__ == "__main__":
    try:
        run_simulation()
    except KeyboardInterrupt:
        print("Simulare oprită.")
    except Exception as e:
        print(f"Eroare: {e}")
    finally:
        producer.close()