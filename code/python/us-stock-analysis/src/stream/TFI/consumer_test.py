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
        .load()
        # .option("startingOffsets", "latest") \
        # .option("failOnDataLoss","false") 

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

    tfi_uc = tfi_uc.select("qls_unit_id", "unit_collection_pt_timestamp","collection_point_id","concern_id","zone_charged_id")

    # # Esto va si muestro los agregados en pantalla
    fail_count = tfi_uc \
    .dropDuplicates(["qls_unit_id","zone_charged_id"]) \
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

    tfi_ucp = tfi_ucp.select("qls_unit_id", "unit_collection_pt_timestamp","collection_point_id", "zone_id")

    #Esto va si muestro los agregados en pantalla
    unit_count = tfi_ucp \
    .dropDuplicates(["qls_unit_id","zone_id"]) \
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

    query_uc = tfi_uc.writeStream \
        .outputMode('append') \
        .format("console") \
        .trigger(processingTime="10 seconds") \
        .start()

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
        # print('\n' + '_' * 30)
        # # interactively query in-memory table
        # spark.sql('SELECT * FROM fail_count').show()
        # # print(query_agg_uc.lastProgress)
        # sleep(10)

#########################################################
# unit_collection_point ( ucp = unit_collection_point ) #
#########################################################

    #############################################
    # muestro a consola los mensajes que llegan #
    #############################################

    query_ucp = tfi_ucp.writeStream \
        .outputMode('append') \
        .format("console") \
        .trigger(processingTime="10 seconds") \
        .start()

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
    #     spark.sql('SELECT zone_id, count as cant_des FROM unit_count order by cant_des DESC').show()
    #     # print(query_agg_ucp2.lastProgress)
    #     sleep(10)

    
    # # si quiero mostrar las dos tablas de agregados las tengo que poner en el mismo while
    while True:
        print('\n' + '_' * 30)
        # interactively query in-memory table
        spark.sql('SELECT zone_charged_id, count as fails FROM fail_count order by fails desc').show()
        # print(query_agg_ucp.lastProgress)
        print('\n' + '_' * 30)
        # interactively query in-memory table
        spark.sql('SELECT zone_id, count as units FROM unit_count order by units DESC').show()
        # print(query_agg_uc.lastProgress)
        sleep(10)     


#####################################
#          Checkpoints              #
#####################################



#     ####################################
#     # Stream to Parquet uc             #
#     ####################################
#     query_chk_uc = tfi_uc \
#         .withColumn('year', year(F.col('unit_collection_pt_timestamp'))) \
#         .withColumn('month', month(F.col('unit_collection_pt_timestamp'))) \
#         .withColumn('day', dayofmonth(F.col('unit_collection_pt_timestamp'))) \
#         .writeStream \
#         .format('parquet') \
#         .partitionBy('year', 'month', 'day') \
#         .option('startingOffsets', 'earliest') \
#         .option('checkpointLocation', '/dataset/checkpoint_tfi_uc') \
#         .option('path', '/dataset/streaming.tfi_uc') \
#         .trigger(processingTime='30 seconds') \
#         .start()

#     ####################################
#     # Stream to Parquet ucp            #
#     ####################################
#     query_chk_ucp = tfi_ucp \
#         .withColumn('year', year(F.col('unit_collection_pt_timestamp'))) \
#         .withColumn('month', month(F.col('unit_collection_pt_timestamp'))) \
#         .withColumn('day', dayofmonth(F.col('unit_collection_pt_timestamp'))) \
#         .writeStream \
#         .format('parquet') \
#         .partitionBy('year', 'month', 'day') \
#         .option('startingOffsets', 'earliest') \
#         .option('checkpointLocation', '/dataset/checkpoint_tfi_ucp') \
#         .option('path', '/dataset/streaming.tfi_ucp') \
#         .trigger(processingTime='30 seconds') \
#         .start()


    ####################################
    # Writing to Postgres
    ####################################

    # Simple insert
    query_pg_uc = stream_to_postgres(tfi_uc,"uc")
    query_pg_ucp = stream_to_postgres(tfi_ucp,"ucp")

    # Average Price Aggregation
    # query_pg_aggr_uc = stream_aggregation_to_postgres_fails(tfi_uc,"fail_totals")
    # query_pg_aggr_ucp = stream_aggregation_to_postgres_units(tfi_ucp,"unit_totals")

##############################################
# A partir de aca dejo loopeando las queries #
##############################################

    query_uc.awaitTermination()
    query_agg_uc.awaitTermination()
    query_ucp.awaitTermination()
    query_agg_ucp.awaitTermination()
    query_pg_uc.awaitTermination()
    query_pg_ucp.awaitTermination()
    # query_pg_aggr_uc.awaitTermination()
    # query_pg_aggr_ucp.awaitTermination()
    
    # Checkpointing
    # query_chk_uc.awaitTermination()
    # query_chk_ucp.awaitTermination()

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


def stream_to_postgres(query, output_table):
    wquery =  (
        query
            .withWatermark("unit_collection_pt_timestamp", "60 seconds")
    )
  

    write_to_postgres_fn = define_write_to_postgres(output_table)

    query = (
        wquery.writeStream
        .foreachBatch(write_to_postgres_fn)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


# Lo que viene aca es para sumarizar
# No funciona bien, ya que se va sobreescribiendo en cada batch (insert vs upsert). No encuentro como hacer
# upsert a postgres

def summarize_fails(query):
    #query tiene que ser tfi_uc
    fail_count = query \
    .dropDuplicates(["qls_unit_id","zone_charged_id"]) \
    .groupBy(F.col("zone_charged_id")) \
    .count().alias("fails")

    fail_count.printSchema()
    return fail_count

def summarize_units(query):
    #query tiene que ser tfi_ucp
    unit_count = query \
    .dropDuplicates(["qls_unit_id","zone_id"]) \
    .groupBy(F.col("zone_id")) \
    .count().alias("units")

    unit_count.printSchema()
    return unit_count


def stream_aggregation_to_postgres_fails(query, output_table):

    fail_count = summarize_fails(query)

    write_to_postgres_fn = define_write_to_postgres(output_table)

    query = (
        fail_count\
        .writeStream
        .foreachBatch(write_to_postgres_fn)
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query

def stream_aggregation_to_postgres_units(query, output_table):

    unit_count = summarize_units(query)

    write_to_postgres_fn = define_write_to_postgres(output_table)

    query = (
        unit_count\
        .writeStream
        .foreachBatch(write_to_postgres_fn)
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


if __name__ == '__main__':
    start_stream(sys.argv)