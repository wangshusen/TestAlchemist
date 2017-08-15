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
        
        val rdd = read.h5read_vec(sc, filepath, "rows", 10).persist()
        val sample = rdd.take(1)(0)
        println("sample type is " + sample.getClass.toString)
        
        val count= rdd.count()
        println(count)
        
        spark.stop
    }
    
    def testSpark(k: Int, sc: SparkContext, vecRdd: RDD[Vector]): Unit = {
        //// Compute the Squared Frobenius Norm
        val sqFroNorm: Double = vecRdd.map(v => Vectors.norm(v, 2))
                                        .map(norm => norm * norm)
                                        .sum
        
        //// Spark Build-in Truncated SVD
        var t1 = System.nanoTime()
        val mat: RowMatrix = new RowMatrix(vecRdd)
        val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(k, computeU=false)
        val v: Matrix = svd.V
        var t2 = System.nanoTime()
        println("Time cost of Spark truncated SVD clustering is " + ((t2 - t1) * 1.0E-9).toString)
        println(" ")
        
        //// Compute Approximation Error
        val vBroadcast = sc.broadcast(v)
        val err: Double = vecRdd
                .map(vec => (vec, vBroadcast.value.transpose.multiply(vec)))
                .map(pair => (pair._1, Vectors.dense(vBroadcast.value.multiply(pair._2).toArray)))
                .map(pair => Vectors.sqdist(pair._1, pair._2))
                .sum
        val relativeError = err / sqFroNorm
        println("Squared Frobenius error of rank " + k.toString + " SVD is " + err.toString)
        println("Squared Frobenius norm of A is " + sqFroNorm.toString)
        println("Relative Error is " + relativeError.toString)
    }
}
