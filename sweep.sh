#!/bin/bash

# Baseline 200ms, 100ms
./run-my-bench.sh 1 200 ad-events-903
./run-my-bench.sh 1 100 ad-events-904

# Drizzle 200ms, 100ms
./run-my-bench.sh 10 2000 ad-events-905
./run-my-bench.sh 20 2000 ad-events-906 

# Drizzle with bigger batch of 400
./run-my-bench.sh 20 4000 ad-events-907
./run-my-bench.sh 40 4000 ad-events-908
