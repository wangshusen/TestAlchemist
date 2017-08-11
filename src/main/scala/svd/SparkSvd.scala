package alchemist.test.svd

// spark-core
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
// spark-sql
import org.apache.spark.sql.SparkSession
// others
import scala.math
// breeze
import breeze.linalg._
import breeze.numerics._

import org.nersc.io._


object SparkSvd {
    def main(args: Array[String]) {
        val projpath: String = "/global/cscratch1/sd/wss/TestAlchemist"
        //val projpath: String = "."
        val filepath: String = "/data/small_data.h5"
        //var logger = LoggerFactory.getLogger(getClass)
        val sc = new SparkContext()
        val rdd = read.h5read(sc, projpath+filepath, "rows", 10)
        rdd.cache()
        val count= rdd.count()
        println(count)
        //logger.info("\nRDD_Count: "+count+" , Total number of rows of all hdf5 files\n")
        sc.stop()
    }
    
}