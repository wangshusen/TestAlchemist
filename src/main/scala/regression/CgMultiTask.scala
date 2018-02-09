package alchemist.test.regression.CgMultiTask

// spark-core
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.broadcast._
// breeze
import breeze.linalg._
import breeze.numerics._

/**
 * Solve a ridge regression problem using conjugate gradient (CG) method. 
 * Model: 0.5/n*||X W - Y||_F^2 + 0.5*gamma*||W||_F^2
 * 
 * @param sc SparkContext
 * @param data RDD of (label, feature)
 */

class Driver(sc: SparkContext, data: RDD[(Array[Double], Array[Double])]) {
    // constants
    val n: Long = data.count
    val k: Int = data.take(1)(0)._1.size
    val d: Int = data.take(1)(0)._2.size
    val nInv: Double = 1.0 / n.toDouble
    
    // variables
    var w: DenseMatrix[Double] = DenseMatrix.zeros[Double](this.d, this.k)
    var g: DenseMatrix[Double] = DenseMatrix.zeros[Double](this.d, this.k)
    
    // initialize executors
    val rdd: RDD[Executor] = data.glom.map(new Executor(_)).persist()
    val xy: DenseMatrix[Double] = rdd.map(_.xy).reduce((a,b) => a+b)
    println("The driver is initialized!")
    
    /**
     * Train the ridge regression model using CG.
     *
     * @param gamma the regularization parameter
     * @param maxIter max number of iterations
     * @return 
     */
    def train(gamma: Double, maxIter: Int): Unit = {
        val t0 = System.nanoTime()
        val ngamma: Double = this.n * gamma
        w := DenseMatrix.zeros[Double](this.d, this.k)
        val wBc: Broadcast[DenseMatrix[Double]] = this.sc.broadcast(w)
        
        // setup the executors for training
        val rddTrain: RDD[Executor] = this.rdd
                                    .map(exe => {exe.setGamma(gamma); exe})
                                    .persist()
        
        val r: DenseMatrix[Double] = DenseMatrix.zeros[Double](this.d, this.k)
        val ap: DenseMatrix[Double] = DenseMatrix.zeros[Double](this.d, this.k)
        val xxw: DenseMatrix[Double] = rddTrain.map(_.xxByP(wBc.value))
                                            .reduce((a,b) => a+b)
        r := this.xy - ngamma * w - xxw // d-by-k
        val p: DenseMatrix[Double] = r.copy  // d-by-k
        val rsold: Array[Double] = new Array[Double](this.k)
        for (i <- 0 until this.k) rsold(i) = r(::, i).toArray.map(a => a*a).sum
        var rsnew: Array[Double] = new Array[Double](this.k)
        
        for (t <- 0 until maxIter) {
            val pBc: Broadcast[DenseMatrix[Double]] = this.sc.broadcast(p)
            val xxp: DenseMatrix[Double] = rddTrain.map(_.xxByP(pBc.value))
                                                .reduce((a,b) => a+b)
            ap := ngamma * p + xxp // d-by-k
            val pap: Array[Double] = new Array[Double](this.k)
            for (i <- 0 until this.k) {
                for (j <- 0 until this.d) {
                    pap(i) += p(j, i) * ap(j, i)
                }
            }
            for (i <- 0 until this.k) {
                val alpha: Double = rsold(i) / pap(i)
                w(::, i) += alpha * p(::, i)
                r(::, i) -= alpha * ap(::, i)
                rsnew(i) = r(::, i).toArray.map(a => a*a).sum
            }
            val rssum: Double = rsnew.toArray.sum
            val t1 = System.nanoTime()
            println("rs = " + rssum.toString + ",  time = " + ((t1-t0)*1E-9).toString)
            
            if (rssum < 1E-20) return 
            
            for (i <- 0 until this.k) p(::, i) := r(::, i) + (rsnew(i) / rsold(i)) * p(::, i)
            for (i <- 0 until this.k) rsold(i) = rsnew(i)
        }
    
        this.w := w
    }
    
    def trainMisclassify(): Double = {
        var wBc: Broadcast[DenseMatrix[Double]] = this.sc.broadcast(this.w)
        val misclassify: Double = this.rdd.map(_.misclassify(wBc.value)).sum
        println("Misclassfication rate is " + (misclassify * this.nInv).toString)
        misclassify
    }
}



/**
 * Perform local computations. 
 * 
 * @param arr array of (label, feature) pairs
 */
class Executor(arr: Array[(Array[Double], Array[Double])]) {
    // get data
    val s: Int = arr.size
    val k: Int = arr(0)._1.size
    val d: Int = arr(0)._2.size
    val y: DenseMatrix[Double] = new DenseMatrix(k, s, arr.map(pair => pair._1).flatten)
    val x: DenseMatrix[Double] = new DenseMatrix(d, s, arr.map(pair => pair._2).flatten)
    val xy: DenseMatrix[Double] = x * y.t
    
    // specific to training
    var gamma: Double = 0.0
    def setGamma(gam: Double): Unit = {
        this.gamma = gam
    }
    
    /**
     * Compute X * X' * P (d-by-k).
     * Here X is d-by-s and P is d-by-k.
     */
    def xxByP(p: DenseMatrix[Double]): DenseMatrix[Double] = {
        val xp: DenseMatrix[Double] = this.x.t * p
        this.x * xp
    }
    
    def misclassify(w: DenseMatrix[Double]): Int = {
        val xw: DenseMatrix[Double] = this.x.t * w
        var err: Int = 0
        for (i <- 0 until this.s) {
            val pred: Int = xw(i, ::).t.argmax
            val label: Int = y(::, i).argmax
            if (pred != label) err += 1
        }
        err
    }
}
