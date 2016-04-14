#!/bin/bash

if [ $# -ne 3 ];
then
  echo "Usage run-my-bench.sh <num-jobs-per-batch> <batch-time> <kafka-topic>"
  exit 0
fi

export NUM_JOBS=$1
export BATCH_TIME=$2
export KAFKA_TOPIC=$3

export LEIN_ROOT="true"

pushd /root/streaming-benchmarks

  # Create a new kafka topic
  pushd kafka_2.10-0.8.2.1

  ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic $KAFKA_TOPIC -partitions 8 --replication-factor 1

  popd

  sed -i 's/spark.jobsperbatch:.*/spark.jobsperbatch: '"$NUM_JOBS"'/g' conf/benchmarkConf.yaml
  sed -i 's/spark.batchtime:.*/spark.batchtime: '"$BATCH_TIME"'/g' conf/benchmarkConf.yaml
  sed -i 's/kafka.topic:.*/kafka.topic: '"$KAFKA_TOPIC"'/g' conf/benchmarkConf.yaml

  # Create the redis data
  pushd data
  lein run -n -a ../conf/benchmarkConf.yaml
  popd

  /root/spark/bin/spark-submit --class spark.benchmark.KafkaRedisAdvertisingStream ./spark-benchmarks/target/spark-benchmarks-0.1.0.jar ./conf/benchmarkConf.yaml >& /mnt/stream-bench-drizzle-"$NUM_JOBS"-jobs-"$BATCH_TIME"-ms.log &

  sleep 60

  ./stream-bench.sh START_LOAD
  sleep 240
  ./stream-bench.sh STOP_LOAD

  sleep 60

  spark_pid=`jps | grep SparkSubmit | awk '{print $1}'`
  kill $spark_pid

  mv data/updated.txt data/updated-"$NUM_JOBS"-jobs-"$BATCH_TIME"-ms.txt

popd
