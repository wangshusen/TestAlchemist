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
DATA_FILE="$PROJ_HOME/data/mnist8m"
RESULT_FILE="$PROJ_HOME/result/kmeans_results"
NUM_CLUSTER="10"

module load spark
module load python/3.5-anaconda
ulimit -s unlimited
start-all.sh

spark-submit \
    --class "alchemist.test.kmeans.SparkKmeans" \
    $JAR_FILE $NUM_CLUSTER $DATA_FILE $RESULT_FILE
    
python $PROJ_HOME/result/kmeans_nmi.py -f $RESULT_FILE \
    >> ResultTestKmeans.out
  
stop-all.sh
