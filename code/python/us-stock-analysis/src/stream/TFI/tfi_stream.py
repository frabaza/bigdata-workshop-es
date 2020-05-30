import sys

from time import sleep

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, year, month, dayofmonth, hour, minute
from pyspark.sql import functions as F  # col doesn't import correctly
from pyspark.sql.types import TimestampType, StringType, StructType, StructField, DoubleType, IntegerType


def validate_params(args):
    if len(args) != 3:
        print(f"""
         |Usage: {args[0]} <brokers> <topics>
         |  <brokers> is a list of one or more Kafka brokers
         |  <topic> is a a kafka topic to consume from
         |
         |  {args[0]} kafka:9092 QLS
        """)
        sys.exit(1)
    pass


def create_spark_session():
    return SparkSession \
        .builder \
        .appName("QLS:Stream:unit_concern") \
        .getOrCreate()


def start_stream(args):
    validate_params(args)
    _, brokers, topic = args

    spark = create_spark_session()

    json = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "smallest") \
        .option("failOnDataLoss","false") \
        .load()
#startingOffsets,latest
    json.printSchema()

    """
    devuelve esto

        root
    |-- key: binary (nullable = true)
    |-- value: binary (nullable = true)
    |-- topic: string (nullable = true)
    |-- partition: integer (nullable = true)
    |-- offset: long (nullable = true)
    |-- timestamp: timestamp (nullable = true)
    |-- timestampType: integer (nullable = true)

    """

    # agrego esta linea

    jsonString = json.selectExpr("CAST(value AS STRING)")

    # Explicitly set schema
    schema = StructType([StructField("qls_unit_id", IntegerType(), False),
                         StructField("unit_collection_pt_timestamp", TimestampType(), False),
                         StructField("unit_collection_pt_timestamp_s", IntegerType(), False),
                         StructField("unit_concern_id", IntegerType(), False),
                         StructField("collection_point_id", IntegerType(), False),
                         StructField("collection_point_section_id", IntegerType(), False),
                         StructField("ii_product_line_group_id", IntegerType(), False),
                         StructField("ii_plg_plant_id", IntegerType(), False),
                         StructField("inspection_item_group_id", IntegerType(), False),
                         StructField("concern_id", IntegerType(), False),
                         StructField("concern_type_id", IntegerType(), False),
                         StructField("position_a_id", IntegerType(), False),
                         StructField("position_b_id", IntegerType(), False),
                         StructField("position_c_id", IntegerType(), False),
                         StructField("position_c_group_id", IntegerType(), False),
                         StructField("position_arbitrary_id", IntegerType(), False),
                         StructField("zone_charged_id", IntegerType(), False),
                         StructField("department_charged_id", IntegerType(), False),
                         StructField("area_charged_id", IntegerType(), False),
                         StructField("external_bridge_id", IntegerType(), False),
                         StructField("repair_external_bridge_id", IntegerType(), False),
                         StructField("plant_unit_location_id", IntegerType(), False),
                         StructField("repair_type_id", IntegerType(), False),
                         StructField("unit_concern_state", StringType(), False),
                         StructField("evaluation_comment", StringType(), False),
                         StructField("drawing_id", IntegerType(), False),
                         StructField("drawing_plant_id", IntegerType(), False),
                         StructField("polygon_id", IntegerType(), False),
                         StructField("defect_x_coordinate", IntegerType(), False),
                         StructField("defect_y_coordinate", IntegerType(), False),
                         StructField("unit_concern_source", StringType(), False),
                         StructField("position_d_id", IntegerType(), False),
                         StructField("position_d_group_id", IntegerType(), False),
                         StructField("online_repair", StringType(), False),
                         StructField("lastupdtby", StringType(), False),
                         StructField("lastupdatedatetime", TimestampType(), False),
                         StructField("plant", StringType(), False),
                         StructField("db_instance", IntegerType(), False)])
                         
    json_options = {"timestampFormat": "yyyy-MM-dd'T'HH:mm'Z'"}

    qls_json = jsonString \
        .select(from_json(F.col("value").cast("string"), schema, json_options).alias("content"))

    qls_json.printSchema()

    qls = qls_json.select("content.*")

    # busco que solo emita las columnas qls_unit_id y unit_collection_pt_timestamp

    qls = qls.select("qls_unit_id", "unit_collection_pt_timestamp","zone_charged_id")


#     # #########################################
#     # Prueba mostrando a Consola
#     # #########################################

    query = qls.writeStream \
        .outputMode('append') \
        .format("console") \
        .trigger(processingTime="10 seconds") \
        .start()
# no puedo usar outputMode complete porque sirve solo para aggregation
    query.awaitTermination()


#     ####################################
#     # Stream to Parquet
#     ####################################
#     query = qls \
#         .withColumn('year', year(F.col('unit_collection_pt_timestamp'))) \
#         .withColumn('month', month(F.col('unit_collection_pt_timestamp'))) \
#         .withColumn('day', dayofmonth(F.col('unit_collection_pt_timestamp'))) \
#         .withColumn('hour', hour(F.col('unit_collection_pt_timestamp'))) \
#         .writeStream \
#         .format('parquet') \
#         .partitionBy('year', 'month', 'day', 'hour') \
#         .option('startingOffsets', 'earliest') \
#         .option('checkpointLocation', '/dataset/checkpoint_qls') \
#         .option('path', '/dataset/streaming.qls') \
#         .trigger(processingTime='30 seconds') \
#         .start()

#     query.awaitTermination()

    ######################################
    # Tabla de agregados por Zone charged
    ######################################

    # fail_count = qls \
    # .groupBy(F.col("zone_charged_id")) \
    # .count().alias("fails")

    # query3 = fail_count \
    #     .writeStream \
    #     .queryName("fail_count") \
    #     .outputMode("complete") \
    #     .format("memory") \
    #     .trigger(processingTime="10 seconds") \
    #     .start()
    
    # while True:
    #     print('\n' + '_' * 30)
    #     # interactively query in-memory table
    #     spark.sql('SELECT * FROM fail_count').show()
    #     print(query3.lastProgress)
    #     sleep(10)

    # query3.awaitTermination()

    ####################################
    # Writing to Postgres
    ####################################

    # Simple insert
    # query = stream_to_postgres(qls)
    # query.awaitTermination()

    # Average Price Aggregation
    # query = stream_aggregation_to_postgres(qls)
    # query.awaitTermination()

    # Final Average Price Aggregation with Timestamp columns
    # query = stream_aggregation_to_postgres_final(qls)
    # query.awaitTermination()

    pass


def define_write_to_postgres(table_name):

    def write_to_postgres(df, epochId):
        return (
            df.write
                .format("jdbc")
                .option("url", "jdbc:postgresql://postgres/workshop")
                .option("dbtable", f"workshop.{table_name}")
                .option("user", "workshop")
                .option("password", "w0rkzh0p")
                .option("driver", "org.postgresql.Driver")
                .mode('append')
                .save()
        )
    return write_to_postgres


def stream_to_postgres(qls, output_table="qls_streaming_inserts"):
    wqls =  (
        qls
            .withWatermark("unit_collection_pt_timestamp", "365 days")
            .select("qls_unit_id", "unit_collection_pt_timestamp", "collection_point_id") \
            .dropDuplicates() #llenar select con las columnas. Crear tabla en Postgres, tirar duplicados prueba
    )
    # ver documentacion. withWatermark no funciona porque los datos son viejos (11-2019) y me borra todo lo que entre mas 
    # atras que 10 segundos de hoy. Pongo 365 dias para debug, no es aplicable a produccion porque tendria que
    # mantener un año de datos en memoria y asi funciona

    write_to_postgres_fn = define_write_to_postgres("qls_streaming_inserts")

    query = (
        wqls.writeStream
        .foreachBatch(write_to_postgres_fn)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


# Lo que viene aca es para sumarizar

def summarize_fails(qls):
    fail_count = (
        qls
        .withWatermark("unit_collection_pt_timestamp", "365 days") \
        .groupBy(hour(F.col("unit_collection_pt_timestamp")).alias("hour"),F.col("collection_point_id")) \
        .count().alias("fails")
    )
    fail_count.printSchema()
    return fail_count


def stream_aggregation_to_postgres(qls, output_table="streaming_inserts_fail_count"):

    fail_count = summarize_fails(qls)

    write_to_postgres_fn = define_write_to_postgres(output_table)

    query = (
        fail_count\
        .writeStream
        .foreachBatch(write_to_postgres_fn)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


def stream_aggregation_to_postgres_final(qls, output_table="streaming_inserts_avg_price_final"):

    fail_count = summarize_fails(qls)

    window_start_ts_fn = F.udf(lambda w: w.start, TimestampType())

    window_end_ts_fn = F.udf(lambda w: w.end, TimestampType())

    write_to_postgres_fn = define_write_to_postgres(output_table)

    query = (
        fail_count\
        .withColumn("window_start", window_start_ts_fn("window"))
        .withColumn("window_end", window_end_ts_fn("window"))
        .drop("window")
        .writeStream
        .foreachBatch(write_to_postgres_fn)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


if __name__ == '__main__':
    start_stream(sys.argv)