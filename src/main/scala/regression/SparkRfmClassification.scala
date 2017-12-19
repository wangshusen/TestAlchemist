package alchemist.test.regression

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
import utils._


object SparkRfmClassification {
    def main(args: Array[String]) {
        //// Parse parameters from command line arguments
        val filepath: String = args(0).toString
        val numFeatures: Int = args(1).toInt
        val gamma: Double = args(2).toDouble
        val numSplits: Int = 100//args(3).toInt
        val numClass: Int = 10
        
        //// Launch Spark
        var t1 = System.nanoTime()
        val spark = (SparkSession
                      .builder()
                      .appName("Test Spark Random Feature Regression")
                      .getOrCreate())
        val sc: SparkContext = spark.sparkContext
        sc.setLogLevel("ERROR")
        var t2 = System.nanoTime()
        println("Time cost of starting Spark session is " + ((t2 - t1) * 1.0E-9).toString)
        println(" ")
        
        //// Load data and perform RFM
        val rddRaw: RDD[(Int, Array[Double])] = SparkRfmClassification
                                                    .loadLibsvmData(spark, filepath, numSplits)
                                                    .persist()
        val rddRfm: RDD[(Int, Array[Double])] = SparkRfmClassification
                                                    .randomFeatureMap(rddRaw, numFeatures)
                                                    .persist()
        
        
        def oneHotEncode(y: Int): Array[Double] = {
            val yArray: Array[Double] = new Array[Double](numClass)
            yArray(y) = 1
            yArray
        }
        val rddOneHot: RDD[(Array[Double], Array[Double])] = rddRfm.map(pair => (oneHotEncode(pair._1), pair._2))
        
        
        //// Train ridge regression by CG
        val cg: CgMultiTask.Driver = new CgMultiTask.Driver(sc, rddOneHot)
        var maxIter: Int = 150
        cg.train(gamma, maxIter)
        cg.trainMisclassify()
        /*
        var results: (Array[Double], Array[Double], Array[Double]) = cg.train(gamma, maxIter)
        println("\n ")
        println("====================================================================")
        println("CG (gamma=" + gamma.toString + ", MaxIter=" + maxIter.toString + ")")
        println("\n ")
        println("Objective Value\t Training Error\t Elapsed Time")
        results.zipped.foreach(this.printAsTable)
        */
        
        spark.stop
    }
    
    def loadLibsvmData(spark: SparkSession, filepath: String, numSplits: Int): RDD[(Int, Array[Double])] = {
        //// Load data from file
        val t1 = System.nanoTime()
        val df = spark.read.format("libsvm").load(filepath)
        val rdd: RDD[(Int, Array[Double])] = df.rdd
                .map(pair => (pair(0).toString.toFloat.toInt, Vectors.parse(pair(1).toString).toArray))
                .persist()
        val count = rdd.count()
        println("n = " + count.toString)
        var t2 = System.nanoTime()
        println("Time cost of loading data is " + ((t2 - t1) * 1.0E-9).toString)
        println(" ")

        rdd
    }
    
    def randomFeatureMap(rdd: RDD[(Int, Array[Double])], numFeatures: Int): RDD[(Int, Array[Double])] = {
        val rdd2: RDD[(Double, Array[Double])] = rdd.map(pair => (pair._1.toDouble, pair._2)).persist()
        //// estimate the kernel parameter (if it is unknown)
        //var sigma: Double = rdd2.glom.map(Kernel.estimateSigma).mean
        //sigma = math.sqrt(sigma)
        //println("Estimated sigma is " + sigma.toString)
        var sigma: Double = 10.16 // MNIST
        
        
        //// Random feature mapping
        val t1 = System.nanoTime()
        val rfmRdd: RDD[(Int, Array[Double])] = rdd2.mapPartitions(Kernel.rbfRfm(_, numFeatures, sigma))
                                            .map(pair => (pair._1.toInt, pair._2))
                                            .persist()
        val s = rfmRdd.take(1)(0)._2.size
        println("s = " + s.toString)
        var t2 = System.nanoTime()
        println("Time cost of feature mapping is " + ((t2 - t1) * 1.0E-9).toString)
        println(" ")
        
        rfmRdd
    }
    
    def printAsTable(element1: Double, element2: Double, element3: Double): Unit = {
        println(element2.toString + "\t" + element1.toString + "\t" + element3.toString)
    }

    
}
