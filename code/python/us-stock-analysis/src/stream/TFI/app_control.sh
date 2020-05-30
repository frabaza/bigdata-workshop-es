#!/usr/bin/env bash

function producer {
  echo "Starting Producer"

  docker exec -w /app/python/us-stock-analysis/src/stream/QLS_TFI worker1 python qls_fake_producer_kafka.py \
   kafka:9092 QLS
}

function consumer {
  echo "Start Consumer with:
  spark-submit \
  --master 'spark://master:7077' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 \
  --jars /app/postgresql-42.1.4.jar \
  /app/python/us-stock-analysis/src/stream/QLS_TFI/consumer_test.py \
  kafka:9092 QLS"

  docker exec -it worker1 bash
}

case $1 in
  producer )
  producer
    ;;

  consumer )
  consumer
    ;;

  * )
  printf "ERROR: Missing command\n  Usage: `basename $0` (producer|consumer)\n"
  exit 1
    ;;
esac
