package alchemist.test.svd

// spark-core
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
// spark-sql
//import org.apache.spark.sql.SparkSession
// others
import scala.math
// breeze
import breeze.linalg._
import breeze.numerics._

import org.nersc.io._


object SparkSvd {
    def main(args: Array[String]) {
        val filepath: String = "/global/cscratch1/sd/wss/mjo/Precipitation_rate_1979_to_1983.h5"
        //var logger = LoggerFactory.getLogger(getClass)
        val sc = new SparkContext()
        val rdd = read.h5read(sc, filepath, "rows", 100)
        rdd.cache()
        val count= rdd.count()
        println(count)
        //logger.info("\nRDD_Count: "+count+" , Total number of rows of all hdf5 files\n")
        sc.stop()
    }
    
}