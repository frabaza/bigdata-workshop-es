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
         |  {args[0]} kafka:9092 TFI
        """)
        sys.exit(1)
    pass


def create_spark_session():
    return SparkSession \
        .builder \
        .appName("TFI:Stream:unit_concern") \
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
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss","false") \
        .load()

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
    jsonString = json.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    json_options = {"timestampFormat": "yyyy-MM-dd'T'HH:mm'Z'"}

#################################################################
# A partir de aca separo el stream dependiendo el key (la tabla)#
#################################################################

#####################################
# unit_concern ( uc = unit_concern )#
#####################################

    # Explicitly set schema unit_concern
    schema_uc = StructType([StructField("qls_unit_id", IntegerType(), False),
                         StructField("unit_collection_pt_timestamp", TimestampType(), False),
                         StructField("collection_point_id", IntegerType(), False),
                         StructField("concern_id", IntegerType(), False),
                         StructField("zone_charged_id", IntegerType(), False)
                         ])


    # cambio jsonString por json
    tfi_json_uc = jsonString \
        .filter(F.col('key') == 'unit_concern') \
        .select(from_json(F.col("value").cast("string"), schema_uc, json_options).alias("content"))

    print('unit_concern schema')
    tfi_json_uc.printSchema()

    tfi_uc = tfi_json_uc.select("content.*")

    # busco que solo emita las columnas qls_unit_id y unit_collection_pt_timestamp

    tfi_uc = tfi_uc.select("qls_unit_id", "unit_collection_pt_timestamp","concern_id","zone_charged_id")

    fail_count = tfi_uc \
    .groupBy(F.col("zone_charged_id")) \
    .count().alias("fails")

#########################################################
# unit_collection_point ( ucp = unit_collection_point ) #
#########################################################

    # Explicitly set schema unit_collection_point
    schema_ucp = StructType([StructField("qls_unit_id", IntegerType(), False),
                         StructField("unit_collection_pt_timestamp", TimestampType(), False),
                         StructField("collection_point_id", IntegerType(), False),
                         StructField("zone_id", IntegerType(), False)
                         ])


    # cambio jsonString por json
    tfi_json_ucp = jsonString \
        .filter(F.col('key') == 'unit_collection_point') \
        .select(from_json(F.col("value").cast("string"), schema_ucp, json_options).alias("content"))

    print('unit_collection_point schema')
    tfi_json_ucp.printSchema()

    tfi_ucp = tfi_json_ucp.select("content.*")

    # busco que solo emita las columnas qls_unit_id y unit_collection_pt_timestamp

    tfi_ucp = tfi_ucp.select("qls_unit_id", "unit_collection_pt_timestamp","zone_id")

    unit_count = tfi_ucp \
    .groupBy(F.col("zone_id")) \
    .count().alias("units")


####################################
# A partir de aca armo las queries #
####################################

#####################################
# unit_concern ( uc = unit_concern )#
#####################################

    #############################################
    # muestro a consola los mensajes que llegan #
    #############################################

    # query_uc = tfi_uc.writeStream \
    #     .outputMode('append') \
    #     .format("console") \
    #     .trigger(processingTime="10 seconds") \
    #     .start()

    #######################################
    # Tabla de agregados por zone_charged #
    #######################################

    query_agg_uc = fail_count \
        .writeStream \
        .queryName("fail_count") \
        .outputMode("complete") \
        .format("memory") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # while True:
    #     print('\n' + '_' * 30)
    #     # interactively query in-memory table
    #     spark.sql('SELECT * FROM fail_count').show()
    #     print(query_agg_uc.lastProgress)
    #     sleep(10)

#########################################################
# unit_collection_point ( ucp = unit_collection_point ) #
#########################################################

    #############################################
    # muestro a consola los mensajes que llegan #
    #############################################

    # query_ucp = tfi_ucp.writeStream \
    #     .outputMode('append') \
    #     .format("console") \
    #     .trigger(processingTime="10 seconds") \
    #     .start()

    #######################################
    # Tabla de agregados por zone_charged #
    #######################################

    query_agg_ucp = unit_count \
        .writeStream \
        .queryName("unit_count") \
        .outputMode("complete") \
        .format("memory") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # while True:
    #     print('\n' + '_' * 30)
    #     # interactively query in-memory table
    #     spark.sql('SELECT * FROM unit_count').show()
    #     print(query_agg_ucp.lastProgress)
    #     sleep(10)

    
    #si quiero mostrar las dos tablas de agregados las tengo que poner en el mismo while
    while True:
        print('\n' + '_' * 30)
        # interactively query in-memory table
        spark.sql('SELECT * FROM unit_count').show()
        print(query_agg_ucp.lastProgress)
        print('\n' + '_' * 30)
        # interactively query in-memory table
        spark.sql('SELECT * FROM fail_count').show()
        print(query_agg_uc.lastProgress)
        sleep(10)     


#     ####################################
#     # Stream to Parquet
#     ####################################
#     query = tfi \
#         .withColumn('year', year(F.col('unit_collection_pt_timestamp'))) \
#         .withColumn('month', month(F.col('unit_collection_pt_timestamp'))) \
#         .withColumn('day', dayofmonth(F.col('unit_collection_pt_timestamp'))) \
#         .withColumn('hour', hour(F.col('unit_collection_pt_timestamp'))) \
#         .writeStream \
#         .format('parquet') \
#         .partitionBy('year', 'month', 'day', 'hour') \
#         .option('startingOffsets', 'earliest') \
#         .option('checkpointLocation', '/dataset/checkpoint_tfi') \
#         .option('path', '/dataset/streaming.tfi') \
#         .trigger(processingTime='30 seconds') \
#         .start()

#     query.awaitTermination()

    ####################################
    # Writing to Postgres
    ####################################

    # Simple insert
    # query = stream_to_postgres(tfi) #la variable de arriba es tfi_uc/tfi_ucp
    # query.awaitTermination()

    # Average Price Aggregation
    # query = stream_aggregation_to_postgres(tfi)
    # query.awaitTermination()

    # Final Average Price Aggregation with Timestamp columns
    # query = stream_aggregation_to_postgres_final(tfi)
    # query.awaitTermination()

##############################################
# A partir de aca dejo loopeando las queries #
##############################################

    # query_uc.awaitTermination()
    query_agg_uc.awaitTermination()
    # query_ucp.awaitTermination()
    query_agg_ucp.awaitTermination()

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


def stream_to_postgres(tfi, output_table="tfi_streaming_inserts"):
    wtfi =  (
        tfi
            .withWatermark("unit_collection_pt_timestamp", "365 days")
            .select("qls_unit_id", "unit_collection_pt_timestamp", "collection_point_id") \
            .dropDuplicates() #llenar select con las columnas. Crear tabla en Postgres, tirar duplicados prueba
    )
    # ver documentacion. withWatermark no funciona porque los datos son viejos (11-2019) y me borra todo lo que entre mas 
    # atras que 10 segundos de hoy. Pongo 365 dias para debug, no es aplicable a produccion porque tendria que
    # mantener un año de datos en memoria y asi funciona

    write_to_postgres_fn = define_write_to_postgres("tfi_streaming_inserts")

    query = (
        wtfi.writeStream
        .foreachBatch(write_to_postgres_fn)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


# Lo que viene aca es para sumarizar

def summarize_fails(tfi):
    fail_count = (
        tfi
        .withWatermark("unit_collection_pt_timestamp", "365 days") \
        .groupBy(hour(F.col("unit_collection_pt_timestamp")).alias("hour"),F.col("collection_point_id")) \
        .count().alias("fails")
    )
    fail_count.printSchema()
    return fail_count


def stream_aggregation_to_postgres(tfi, output_table="streaming_inserts_fail_count"):

    fail_count = summarize_fails(tfi)

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


def stream_aggregation_to_postgres_final(tfi, output_table="streaming_inserts_avg_price_final"):

    fail_count = summarize_fails(tfi)

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