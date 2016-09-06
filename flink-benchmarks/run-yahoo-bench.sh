#!/bin/bash

pushd ../flink-1.1.1

./bin/stop-cluster.sh

./bin/start-cluster.sh

PARTS=28
BENCH_CLASS="flink.benchmark.AdvertisingTopologyFlinkWindows"
LOAD_TARGET=3200000 # 3.2M
TOTAL_NUM_ELEMENTS=960000000 # 960M
CKPT_INTERVAL=1000

if [[ $# -ne 1 ]]
then
  echo "Usage <run.sh> suffix"
  exit 0
fi

SUFFIX=$1

OUT_PATH="/tmp/adOuts-"$LOAD_TARGET"-load-"$TOTAL_NUM_ELEMENTS"-total-"$SUFFIX

./bin/flink run --parallelism $PARTS --class $BENCH_CLASS ../flink-benchmarks/target/flink-benchmarks-0.1.0.jar --outPath $OUT_PATH --loadTargetHz $LOAD_TARGET --totalNumElems $TOTAL_NUM_ELEMENTS --flinkCheckpointInterval $CKPT_INTERVAL > /mnt/flink-"$LOAD"-load-"$TOTAL_NUM_ELEMENTS"-elems-"$PARTS"-parts-"$CKPT_INTERVAL"-ckpt-"$SUFFIX".log 2>&1 &

echo "Sleeping for 120 seconds"

sleep 120

for i in `cat /root/spark/conf/slaves`; do echo "$RANDOM $i"; done | sort | awk '{print $2}' > /tmp/random-slaves
SLAVE_TO_KILL=`head -1 /tmp/random-slaves`
echo "Killing executor in $SLAVE_TO_KILL"
ssh $SLAVE_TO_KILL '/root/streaming-benchmarks/kill_flink_exec.sh'

wait
