# Lambda Architecture - E-commerce Events

Proyecto de ejemplo de **Lambda Architecture** para procesar eventos de e-commerce con una capa de **speed**, una capa de **batch** y una **serving layer** para consulta y visualización.

## Descripción general

El proyecto simula un flujo de eventos de e-commerce y los procesa en paralelo en dos caminos:

- **Speed Layer**: consume eventos desde Kafka con Spark Structured Streaming y mantiene agregados en tiempo real en Redis.
- **Batch Layer**: persiste los eventos raw en HDFS mediante Kafka Connect.
- **Serving Layer**: expone dashboards en Streamlit consultando Redis para realtime y Hive para histórico.
- **Lambda View**: compara la vista batch con la vista speed en una misma interfaz.

## Flujo general

1. `generate_events.py` genera eventos dummy de e-commerce en formato JSON.
2. Los eventos ingresan a Kafka en el tópico `ecommerce-events`.
3. `speed/stream.py` consume el stream, agrupa por `product_id` y `event_name`, y escribe conteos en Redis.
4. Kafka Connect, usando `connectors/hdfs-sink.json`, guarda los eventos raw en HDFS.
5. `serving/serving.py` centraliza la conexión y acceso a Redis y Hive.
6. `serving/streamlit_app.py` construye la interfaz analítica con vistas realtime, históricas y comparativas.

## Estructura del proyecto

```bash
Integration/
├── docker-compose.yml
├── generate_events.py
├── connect/
│   └── Dockerfile
├── connectors/
│   └── hdfs-sink.json
├── speed/
│   └── stream.py
├── serving/
│   ├── serving.py
│   ├── streamlit_app.py
│   ├── requirements.txt
│   └── .env
└── config/
    ├── hadoop/
    └── hive/
```

## Componentes principales

### `docker-compose.yml`
Define la infraestructura completa del entorno:

- Zookeeper
- Kafka
- Redis
- HDFS (NameNode y DataNode)
- Kafka Connect
- Spark Streaming
- Hive Metastore
- HiveServer2

### `generate_events.py`
Generador de eventos dummy de e-commerce. Produce eventos como:

- `product_view`
- `click`
- `add_to_cart`
- `update_cart`
- `remove_from_cart`
- `begin_checkout`
- `checkout_progress`
- `purchase`

Los eventos incluyen información de producto, usuario, carrito, checkout, revenue y timestamps en UTC.

### `connect/Dockerfile`
Extiende la imagen de Kafka Connect e instala el conector **HDFS Sink**, necesario para persistir eventos en HDFS.

### `connectors/hdfs-sink.json`
Configuración del conector que toma mensajes del tópico `ecommerce-events` y los escribe en HDFS en formato JSON bajo `/data/raw/events`.

### `speed/stream.py`
Implementa la **Speed Layer** con Spark Structured Streaming.

- Lee eventos desde Kafka
- Parsea el JSON
- Convierte `event_time` a timestamp
- Filtra eventos válidos
- Agrega por `product_id` y `event_name`
- Guarda los resultados en Redis con claves tipo:

```text
realtime:stats:<product_id>
```

### `serving/serving.py`
Módulo de acceso a datos para la Serving Layer.

- Define configuración centralizada mediante `Settings`
- Conecta con Redis
- Recupera estadísticas realtime
- Ejecuta consultas SQL en Hive
- Genera una consulta base para histórico
- Documenta la lógica de la Lambda View actual

### `serving/streamlit_app.py`
Dashboard principal del proyecto.

Incluye tres vistas:

- **Realtime (Redis)**: resumen, top eventos, top productos, drilldown por producto, paretos y heatmaps
- **Histórico (Hive)**: ranking por evento, series temporales, heatmaps y distribución por hora
- **Lambda View**: comparación entre conteos batch y realtime por `event_name`

También incorpora filtros, caché, descarga de CSV y SQL editable para consultas batch.

### `config/hadoop/`
Contiene la configuración necesaria para que HDFS y los servicios relacionados funcionen dentro del entorno Docker.

### `config/hive/`
Contiene la configuración de Hive usada por metastore y HiveServer2.

## Nota sobre la Lambda View

La comparación batch vs speed está implementada como:

- **Histórico desde Hive**
- **Snapshot acumulado desde Redis**

Actualmente Redis guarda conteos acumulados por `product_id + event_name`, **sin ventana temporal**.  
Por eso, la Lambda View representa una comparación funcional entre ambas capas, pero no una reconciliación temporal exacta.

## Tecnologías

- Python
- Apache Kafka
- Spark Structured Streaming
- Redis
- HDFS
- Kafka Connect
- Hive
- Streamlit
- Docker
