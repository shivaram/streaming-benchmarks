#!/bin/bash

pushd ../flink-1.1.1

PARTS=28
ITERS=100
ELEMS_PER_ITER=100000
BENCH_CLASS="flink.benchmark.StreamMicroBenchmark"
FLINK_BUFFER_TIMEOUT=100
FLINK_CKPT_INTERVAL=1000

TOTAL_ELEMS=$(( $PARTS * $ITERS * $ELEMS_PER_ITER ))

if [[ $# -ne 1 ]]
then
  echo "Usage <run.sh> suffix"
  exit 0
fi

NUM_TRIALS=5
SUFFIX=$1

for window in 50 #100 500 1000
do
  ~/spark/sbin/slaves.sh rm -rf /tmp/out.csv*

  ./bin/flink run --parallelism $PARTS --class $BENCH_CLASS ../flink-benchmarks/target/flink-benchmarks-0.1.0.jar --iterations $ITERS --elemsPerIter $ELEMS_PER_ITER --windowTimeInMs $window --flinkBufferTimeout $FLINK_BUFFER_TIMEOUT --flinkCheckpointInterval $FLINK_CKPT_INTERVAL --numTrials $NUM_TRIALS >& /mnt/flink-"$TOTAL_ELEMS"-window-"$window"-parts-"$PARTS"-bufferT-"$FLINK_BUFFER_TIMEOUT"-ckptT-"$FLINK_CKPT_INTERVAL"-"$SUFFIX".log

  for trial in `seq 1 $NUM_TRIALS`
  do
    ~/spark/sbin/slaves.sh cat /tmp/out.csv-$trial/* > /mnt/flink-"$TOTAL_ELEMS"-window-"$window"-parts-"$PARTS"-trial-"$trial"-"$SUFFIX".out.csv
  done
done

popd
