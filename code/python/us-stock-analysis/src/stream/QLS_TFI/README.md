### QLS app para FTI

### Comenzar fake generator
```bash
docker exec -it worker1 bash

cd /app/python/us-stock-analysis/src/stream/QLS_TFI

# generate stream data
python qls_fake_producer.py kafka:9092 QLS
```

### Process using Spark Structured Stream API

Abrir otra tab y volver a ingresar al servidor donde se encuentran corriendo los contenedores.
Luego, para correr la aplicación de spark conectarse a un worker, ir al directorio con el código y correr `spark-submit` de la siguiente manera:

```bash
docker exec -it worker1 bash

cd /app/python/us-stock-analysis/

spark-submit \
  --master 'spark://master:7077' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 \
  --jars /app/postgresql-42.1.4.jar \
  src/stream/QLS_TFI/consumer_test.py \
  kafka:9092 QLS
```

#borrar

spark-submit \
  --master 'spark://master:7077' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 \
  --jars /app/postgresql-42.1.4.jar \
  src/stream/QLS_TFI/test_sin_parsear.py \
  kafka:9092 QLS






Crear las tablas que vamos a utilizar para el ejercicio con los siguientes comandos (copiar el comando entero, pegar y presionar enter por cada uno)

```sql
CREATE table qls_streaming_inserts (
"qls_unit_id" integer not null,
"unit_collection_pt_timestamp" timestamptz NOT NUll,
"collection_point_id" integer not null);
);
```

```sql
CREATE TABLE streaming_inserts_fail_count (
    "window" varchar(128),
    "collection_point_id" integer,
    fail_count integer
);
```

```sql
CREATE TABLE streaming_inserts_avg_price_final (
    window_start timestamp,
    window_end timestamp,
    symbol varchar(10),
    avg_price real
);
```