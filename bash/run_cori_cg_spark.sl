#!/bin/bash
#SBATCH -p regular
#SBATCH -N 60
#SBATCH -C haswell
#SBATCH -t 00:30:00
#SBATCH -J cg_timit
#SBATCH -L SCRATCH
#SBATCH -e cg_timit_job_%j.err
#SBATCH -o cg_timit_job_%j.out


PROJ_HOME="$SCRATCH/TestAlchemist"
JAR_FILE="$PROJ_HOME/target/scala-2.11/testalchemist_2.11-1.0.jar"
#DATA_FILE="$PROJ_HOME/data/mnist8m"
DATA_FILE="/global/cscratch1/sd/wss/data_timit/timit-train.csv"
NUM_FEATURE="10000"
REG_PARAM="1E-5"
NUM_SPLIT="599"

module load spark
ulimit -s unlimited
start-all.sh

spark-submit \
    --class "alchemist.test.regression.SparkRfmClassification" \
    --num-executors $NUM_SPLIT \
    --driver-cores 3 \
    --executor-cores 3 \
    --driver-memory 10G \
    --executor-memory 10G \
    $JAR_FILE $DATA_FILE $NUM_FEATURE $REG_PARAM $NUM_SPLIT
    
stop-all.sh
