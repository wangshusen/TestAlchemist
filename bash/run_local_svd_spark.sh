#!/usr/bin/env bash

PROJ_HOME="$HOME/Code/TestAlchemist"
JAR_FILE="$PROJ_HOME/target/scala-2.11/testalchemist_2.11-1.0.jar"
DATA_FILE="$HOME/mjo/Precipitation_rate_1979_to_1983_subset.h5"
K="10"
MASTER="local[4]"
NUM_FEATURE="50"

module load spark

spark-submit \
    --class "alchemist.test.svd" \
    --master $MASTER \
    --driver-memory 8G \
    --executor-cores 1 \
    --executor-memory 8G \
    --num-executors $NUM_SPLITS \
    $JAR_FILE $DATA_FILE $K \
    > Result_Spark_SVD.out
  
  