package alchemist.test.svd

// spark-core
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
// spark-sql
import org.apache.spark.sql.SparkSession
// spark-mllib
import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrix, Matrices}
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

// others
import scala.math
import java.io._


import org.nersc.io._


object SparkKmeans {
    def main(args: Array[String]) {
        //// Parse parameters from command line arguments
        val k: Int = args(0).toInt
        val filepath: String = args(1).toString
        val outpath: String = args(2).toString
        
        //// Launch Spark
        var t1 = System.nanoTime()
        val spark = (SparkSession
                      .builder()
                      .appName("Test Spark KMeans")
                      .getOrCreate())
        val sc: SparkContext = spark.sparkContext
        sc.setLogLevel("ERROR")
        var t2 = System.nanoTime()
        println("Time cost of starting Spark session is " + ((t2 - t1) * 1.0E-9).toString)
        println(" ")
        
        //// Load data
        val df = spark.read.format("libsvm").load(filepath)
        val rdd: RDD[(Int, Vector)] = df.rdd
                .map(pair => (pair(0).toString.toFloat.toInt, Vectors.parse(pair(1).toString)))
                .persist()
        val count= rdd.count()
        println("n = " + count.toString)
        var t3 = System.nanoTime()
        println("Time cost of loading data is " + ((t3 - t2) * 1.0E-9).toString)
        println(" ")
        
        val labelStr: String = SparkKmeans.runKmeans(sc, rdd, k, 20)
        val writer = new PrintWriter(new File(outpath))
        writer.write(labelStr)
        writer.close()

        spark.stop
    }
    
    def runKmeans(sc: SparkContext, label_vector_rdd: RDD[(Int, Vector)], k: Int, maxiter: Int): String = {
        // K-Means Clustering
        val t1 = System.nanoTime()
        val clusters = KMeans.train(label_vector_rdd.map(pair => pair._2), k, maxiter)
        val broadcast_clusters = sc.broadcast(clusters)
        val labels: Array[String] = label_vector_rdd
                .map(pair => (pair._1, broadcast_clusters.value.predict(pair._2)))
                .map(pair => pair._1.toString + " " + pair._2.toString)
                .collect()
        val t2 = System.nanoTime()
        
        // Print Info
        println("Time cost of Spark k-means clustering: ")
        println((t2 - t1) * 1.0E-9)
        println(" ")
        
        val label_str: String = (labels mkString " ").trim
        label_str
    }

}
