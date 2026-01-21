# IoT Data Streaming Pipeline

Pipeline de streaming în timp real pentru date IoT folosind Kafka, MongoDB Atlas și Metabase.

## Arhitectură

```
CSV Data → Kafka Producer → Kafka → Stream Processor → MongoDB Atlas → Metabase
```

**Componente:**
- **Sensor Producer** - citește date din CSV și le publică în Kafka
- **Stream Processor** - validează datele și detectează anomalii
- **MongoDB Sink** - salvează datele procesate în MongoDB Atlas
- **Metabase** - vizualizare și dashboards

---

## Pas 1: Instalare Dependențe

### 1.1 Instalează Docker Desktop
Descarcă și instalează [Docker Desktop](https://www.docker.com/products/docker-desktop/)

### 1.2 Instalează Python dependencies
```bash
pip install kafka-python pymongo pandas
```

---

## Pas 2: Pornește Infrastructura (Docker)

```bash
cd config
docker-compose up -d
```

Verifică că toate containerele rulează:
```bash
docker-compose ps
```

Ar trebui să vezi:
| Container | Port | Descriere |
|-----------|------|-----------|
| zookeeper | 2181 | Coordonator Kafka |
| kafka | 9092 | Message broker |
| metabase | 3001 | Vizualizare |

---

## Pas 3: Generează Date Demo (opțional)

Dacă nu ai date, generează un set de test:

```bash
python scripts/generate_demo_data.py 1000
```

Aceasta creează `data/iot_demo_data.csv` cu 1000 de înregistrări.

---

## Pas 4: Rulează Pipeline-ul

### Opțiunea A: Rulare automată (recomandat)
```bash
python run_pipeline.py
```
Aceasta pornește toate cele 3 componente automat. Apasă `Ctrl+C` pentru a opri.

### Opțiunea B: Rulare manuală (în terminale separate)

**Terminal 1 - Producer:**
```bash
python scripts/Sensor_prooducer.py
```

**Terminal 2 - Processor:**
```bash
python scripts/processing_data.py
```

**Terminal 3 - MongoDB Sink:**
```bash
python scripts/kafta_MongoAtlas.py
```

---

## Pas 5: Configurează Metabase

### 5.1 Deschide Metabase
Accesează în browser: **http://localhost:3001**

### 5.2 Creează cont admin
La prima accesare, completează:
- Nume, email, parolă
- Click **Next** / **Skip** pentru restul pașilor

### 5.3 Adaugă baza de date MongoDB

1. Mergi la **Settings** (⚙️ în dreapta sus) → **Admin settings**
2. Click **Databases** → **Add database**
3. Selectează **MongoDB**
4. Completează:

| Câmp | Valoare |
|------|---------|
| Display name | `iot-pipeline` |
| Host | `cluster0.d0wtmr3.mongodb.net` |
| Database name | `iot` |
| Username | `proiectibd_db_user` |
| Password | `Fq20620LQNSFLUKY` |
| Use SSL | ✅ Enabled |
| Authentication database | `admin` |

5. Click **Save**

---

## Pas 6: Creează Vizualizări în Metabase

### 6.1 Grafic Temperature (Line Chart)

1. Click **+ New** → **Question**
2. Selectează **iot-pipeline** → **iot_processed**
3. Click **Summarize** (dreapta sus):
   - **Pick a metric**: `Average of temp`
   - **Group by**: `ts`
4. Click **Visualize**
5. Schimbă vizualizarea la **Line** (dacă nu e deja)
6. Click **Save** → dă-i un nume → salvează

### 6.2 Grafic Humidity (Line Chart)

1. **+ New** → **Question** → **iot_processed**
2. **Summarize**:
   - **Metric**: `Average of humidity`
   - **Group by**: `ts`
3. **Visualize** → **Line** → **Save**

### 6.3 Alert Distribution (Pie Chart)

1. **+ New** → **Question** → **iot_alerts**
2. **Summarize**:
   - **Metric**: `Count`
   - **Group by**: `alert_type`
3. **Visualize** → schimbă la **Pie**
4. **Save**

### 6.4 Alerts Table

1. **+ New** → **Question** → **iot_alerts**
2. **Sort**: `ts` descending
3. **Row limit**: 100
4. **Visualize** → rămâne **Table**
5. **Save**

### 6.5 Latest Readings per Device

1. **+ New** → **Question** → **iot_processed**
2. **Summarize**:
   - **Metric**: `Average of temp`, `Average of humidity`
   - **Group by**: `device_id`
3. **Visualize** → **Bar** sau **Table**
4. **Save**

---

## Pas 7: Creează Dashboard

1. Click **+ New** → **Dashboard**
2. Dă-i un nume: `IoT Monitoring`
3. Click **+** → **Existing question**
4. Adaugă toate vizualizările create
5. Aranjează-le pe ecran (drag & drop)
6. Click **Save**

# Recent Alerts (Table)

[
  { "$sort": { "ts": -1 } },
  { "$limit": 100 },
  { "$project": {
      "timestamp": "$ts",
      "alert_type": 1,
      "device_id": 1,
      "value": 1
  }}
]

# Average Temperature by Device (Bar Chart)
[
  { "$group": {
      "_id": "$device_id",
      "avg_temp": { "$avg": "$temp" },
      "avg_humidity": { "$avg": "$humidity" }
  }}
]


# Alert Counts by Type (Bar/Pie Chart)
[
  { "$group": {
      "_id": "$alert_type",
      "count": { "$sum": 1 }
  }},
  { "$sort": { "count": -1 } }
]
---

## Structura Proiectului

```
pipeline-bigdata/
├── config/
│   └── docker-compose.yml      # Kafka + Zookeeper + Metabase
├── data/
│   └── iot_demo_data.csv       # Date de test
├── scripts/
│   ├── Sensor_prooducer.py     # Producer: CSV → Kafka
│   ├── processing_data.py      # Processor: validare + alerte
│   ├── kafta_MongoAtlas.py     # Sink: Kafka → MongoDB
│   └── generate_demo_data.py   # Generator date demo
├── run_pipeline.py             # Rulare automată pipeline
└── README.md
```

---

## Kafka Topics

| Topic | Descriere |
|-------|-----------|
| `iot_sensors` | Date raw de la senzori |
| `iot_processed` | Date validate și curățate |
| `iot_alerts` | Evenimente de alertă |

---

## Tipuri de Alerte

| Alertă | Condiție |
|--------|----------|
| `SMOKE_HIGH` | smoke > 0.02 |
| `CO_HIGH` | co > 0.01 |
| `LPG_HIGH` | lpg > 0.01 |
| `SUS_MOTION` | motion=true ȘI light=false |

---

## Oprire Pipeline

### Oprește scripturile Python
Apasă `Ctrl+C` în terminal

### Oprește containerele Docker
```bash
cd config
docker-compose down
```

---

## Troubleshooting

### Metabase nu se conectează la MongoDB
- Verifică că SSL este activat
- Authentication database trebuie să fie `admin`
- Verifică username/password

### Kafka connection refused
- Așteaptă 30 secunde după `docker-compose up`
- Verifică: `docker-compose ps` - toate containerele trebuie să fie "Up"

### Nu apar date în Metabase
- Verifică că pipeline-ul rulează (vezi output în terminal)
- În Metabase: **Settings** → **Admin** → **Databases** → click pe db → **Sync database schema**
