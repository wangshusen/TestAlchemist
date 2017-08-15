#!/bin/bash
#SBATCH -p debug
#SBATCH -N 2
#SBATCH -C haswell
#SBATCH -t 02:00:00
#SBATCH -J wss_giant
#SBATCH -L SCRATCH
#SBATCH -e giant_job_%j.err
#SBATCH -o giant_job_%j.out

PROJ_HOME="$SCRATCH/TestAlchemist"
JAR_FILE="$PROJ_HOME/target/scala-2.11/testalchemist_2.11-1.0.jar"
DATA_FILE="$SCRATCH/mjo/Precipitation_rate_1979_to_1983_subset.h5"
K="10"

module load spark
ulimit -s unlimited
start-all.sh

spark-submit \
    --class "alchemist.test.svd.SparkSvd" \
    $JAR_FILE $DATA_FILE $K
  
stop-all.sh
