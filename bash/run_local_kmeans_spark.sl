#!/usr/bin/env bash
# assume spark-submit is in path

# user specified
PROJ_HOME="$HOME/Code/TestAlchemist"
MASTER="local[3]"
NUM_CLUSTER="10"

# the rest does not need change

# data
DATA_FILE="$PROJ_HOME/data/mnist"

# output
RESULT_FILE="$PROJ_HOME/result/kmeans_results"

# .jar file
JAR_FILE="$PROJ_HOME/target/scala-2.11/testalchemist_2.11-1.0.jar"

spark-submit \
    --master $MASTER \
    --class "alchemist.test.svd.SparkKmeans" \
    $JAR_FILE $NUM_CLUSTER $DATA_FILE $RESULT_FILE \
    > ResultTestKmeans.out
    
python $PROJ_HOME/result/kmeans_nmi.py -f $RESULT_FILE \
    >> ResultTestKmeans.out