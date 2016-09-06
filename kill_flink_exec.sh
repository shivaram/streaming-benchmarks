#!/bin/bash

PID_TO_KILL=`jps | grep TaskManager | awk '{print $1}'`

if [[ ! -z ${PID_TO_KILL} ]]
then
  echo "Killing $PID_TO_KILL"
  kill -9 $PID_TO_KILL
fi
