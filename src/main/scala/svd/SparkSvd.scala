package alchemist.test.svd

// spark-core
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
// spark-sql
import org.apache.spark.sql.SparkSession
// spark-mllib
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrix, Matrices}
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.SingularValueDecomposition
//breeze
import breeze.linalg.{DenseVector => BDV, max, min, DenseMatrix => BDM, norm, diag, svd}
import breeze.numerics._
// others
import scala.math
import java.io._


import org.nersc.io._


object SparkSvd {
    def main(args: Array[String]) {
        //// parse parameters from command line arguments
        val filepath: String = args(0).toString
        val k: Int = args(1).toInt
        
        //// Launch Spark
        var t1 = System.nanoTime()
        val spark = (SparkSession
                      .builder()
                      .appName("Test Spark Truncated SVD")
                      .getOrCreate())
        val sc: SparkContext = spark.sparkContext
        sc.setLogLevel("ERROR")
        var t2 = System.nanoTime()
        println("Time cost of starting Spark session is " + ((t2 - t1) * 1.0E-9).toString)
        println(" ")
        
        //val filepath: String = "/global/cscratch1/sd/wss/TestAlchemist/data/small_data.h5"
	    //val filepath: String = "/global/cscratch1/sd/wss/mjo/Precipitation_rate_1979_to_1983.h5"
        val rdd = read.h5read_irow(sc, filepath, "rows", 10).persist()
        val count= rdd.count()
        println(count)
        rdd.take(5).foreach(println)
        
        spark.stop
    }
    
    def testSpark(k: Int, sc: SparkContext, labelVecRdd: RDD[(Int, Vector)]): Unit = {
        //// Compute the Squared Frobenius Norm
        val sqFroNorm: Double = labelVecRdd.map(pair => Vectors.norm(pair._2, 2))
                                        .map(norm => norm * norm)
                                        .sum
        
        //// Spark Build-in Truncated SVD
        var t1 = System.nanoTime()
        val mat: RowMatrix = new RowMatrix(labelVecRdd.map(pair => pair._2))
        val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(k, computeU=false)
        val v: Matrix = svd.V
        var t2 = System.nanoTime()
        println("Time cost of Spark truncated SVD clustering is " + ((t2 - t1) * 1.0E-9).toString)
        println(" ")
        
        //// Compute Approximation Error
        val vBroadcast = sc.broadcast(v)
        val err: Double = labelVecRdd
                .map(pair => (pair._2, vBroadcast.value.transpose.multiply(pair._2)))
                .map(pair => (pair._1, Vectors.dense(vBroadcast.value.multiply(pair._2).toArray)))
                .map(pair => Vectors.sqdist(pair._1, pair._2))
                .reduce((a, b) => a + b)
        val relativeError = err / sqFroNorm
        println("Squared Frobenius error of rank " + k.toString + " SVD is " + err.toString)
        println("Squared Frobenius norm of A is " + sqFroNorm.toString)
        println("Relative Error is " + relativeError.toString)
    }
}
