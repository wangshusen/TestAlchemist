#!/usr/bin/env bash
# assume spark-submit is in path

# user specified
PROJ_HOME="$HOME/Code/TestAlchemist"
NUM_SPLIT="1000"
NUM_FEATURE="300"
REG_PARAM='1E-5'
MASTER="local["$NUM_SPLIT"]"

# the rest does not need change

# data
# DATA_FILE="$PROJ_HOME/data/mnist.scale.t"
DATA_FILE=$PROJ_HOME"/data/timit-test.csv"

# .jar file
JAR_FILE="$PROJ_HOME/target/scala-2.11/testalchemist_2.11-1.0.jar"

spark-submit \
    --master $MASTER \
    --class "alchemist.test.regression.SparkRfmClassification" \
    $JAR_FILE $DATA_FILE $NUM_FEATURE $REG_PARAM $NUM_SPLIT \
    > ResultTestClassificationRfm.out

