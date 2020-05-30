### QLS app para FTI

### Comenzar fake generator
```bash
docker exec -it worker1 bash

cd /app/python/us-stock-analysis/src/stream/TFI

# generate stream data
python tfi_fake_producer.py kafka:9092 TFI
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
  src/stream/TFI/consumer_test.py \
  kafka:9092 TFI
```

Crear las tablas que vamos a utilizar para el ejercicio con los siguientes comandos (copiar el comando entero, pegar y presionar enter por cada uno)

```bash
./control-env.sh psql
```

```sql
CREATE table uc (
"qls_unit_id" integer NOT NULL,
"unit_collection_pt_timestamp" timestamptz NOT NUll,
"collection_point_id" integer NOT NULL,
"concern_id" integer NOT NULL,
"zone_charged_id" integer NOT NULL
);

CREATE table ucp (
"qls_unit_id" integer NOT NULL,
"unit_collection_pt_timestamp" timestamptz NOT NUll,
"collection_point_id" integer NOT NULL,
"zone_id" integer NOT NULL
);
```

```sql
CREATE TABLE streaming_inserts_fail_count (
  "zone_charged_id" integer NOT NULL,
  "fails" integer
);

CREATE TABLE streaming_inserts_unit_count (
  "zone_id" integer NOT NULL,
  "units" integer
);
```

