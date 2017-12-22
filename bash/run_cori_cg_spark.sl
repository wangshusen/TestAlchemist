#!/bin/bash
#SBATCH -p regular
#SBATCH -N 10
#SBATCH -C haswell
#SBATCH -t 03:00:00
#SBATCH -J wss_giant
#SBATCH -L SCRATCH
#SBATCH -e giant_job_%j.err
#SBATCH -o giant_job_%j.out


PROJ_HOME="$SCRATCH/TestAlchemist"
JAR_FILE="$PROJ_HOME/target/scala-2.11/testalchemist_2.11-1.0.jar"
#DATA_FILE="$PROJ_HOME/data/mnist8m"
DATA_FILE="/global/cscratch1/sd/wss/data_timit/timit-test.csv"
NUM_FEATURE="5000"
REG_PARAM='1E-8'

module load spark
ulimit -s unlimited
start-all.sh

spark-submit \
    --class "alchemist.test.regression.SparkRfmClassification" \
    $JAR_FILE $DATA_FILE $NUM_FEATURE $REG_PARAM
    
stop-all.sh
