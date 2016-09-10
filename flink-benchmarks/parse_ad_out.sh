#!/bin/bash

if [[ $# -ne 2 ]]
then
  echo "Usage <parse-csv.sh> <path> <final-out>"
  exit 1
fi

COMBINED_OUT_FILE="$PWD/ad-out-parsed.txt"
rm $COMBINED_OUT_FILE
PATH_TO_COPY=$1

for slave in `cat /root/spark-ec2/slaves`
do

  rm -rf /tmp/$slave
  mkdir -p /tmp/$slave

  scp -r $slave:$PATH_TO_COPY /tmp/$slave/ 
  scp -r $slave:"$PATH_TO_COPY.old" /tmp/$slave/ 
  pushd /tmp/$slave 
  for f in `ls /tmp/$slave`
  do
    for g in `ls $f`
    do
      cat $f/$g | awk -F',' '{if (NF==3) print $0}' >> $COMBINED_OUT_FILE
    done
  done
  popd
done

cat $COMBINED_OUT_FILE | awk -F',' '{
  cid=$1;
  window=$2;
  delay=$3;
  key=cid","window
  if (delay > results[key]) {
    results[key] = delay
  }
} END {
  for (k in results) {
    print k","results[k]
  }
}' > $2

echo "Created output file $2"


#python parse_combined_output.py $COMBINED_OUT_FILE
