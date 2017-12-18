#!/bin/bash
#SBATCH -p debug
#SBATCH -N 3
#SBATCH -C haswell
#SBATCH -t 00:15:00
#SBATCH -J wss_giant
#SBATCH -L SCRATCH
#SBATCH -e giant_job_%j.err
#SBATCH -o giant_job_%j.out


PROJ_HOME="$SCRATCH/TestAlchemist"
JAR_FILE="$PROJ_HOME/target/scala-2.11/testalchemist_2.11-1.0.jar"
DATA_FILE="$PROJ_HOME/data/mnist"
NUM_FEATURE="100"
REG_PARAM='1E-2'

module load spark
ulimit -s unlimited
start-all.sh

spark-submit \
    --class "alchemist.test.regression.SparkRfmClassification" \
    $JAR_FILE $DATA_FILE $NUM_FEATURE $REG_PARAM
    
stop-all.sh
