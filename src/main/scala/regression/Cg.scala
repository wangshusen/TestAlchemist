package alchemist.test.regression.Cg

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
 * Model: 0.5/n*||X w - y||_2^2 + 0.5*gamma*||w||_2^2
 * 
 * @param sc SparkContext
 * @param data RDD of (label, feature)
 * @param isModelAvg is true if model averaging is used to initialize w
 */
class Driver(sc: SparkContext, data: RDD[(Double, Array[Double])], isModelAvg: Boolean = false) {
    val isMute: Boolean = false
    // constants
    val n: Long = data.count
    val d: Int = data.take(1)(0)._2.size
    val m: Long = data.getNumPartitions
    val nInv: Double = 1.0 / n.toDouble
    val gNormTol: Double = 1.0E-18 * d.toDouble
    
    // variables
    var w: Array[Double] = new Array[Double](d)
    var g: Array[Double] = new Array[Double](d)
    var p: Array[Double] = new Array[Double](d)
    var trainError: Double = 1.0E10
    var objVal: Double = 1.0E10
    var gNorm: Double = 1.0
            
    // initialize executors
    val rdd: RDD[Executor] = data.glom.map(new Executor(_)).persist()

    /**
     * Train a ridge regression model using GIANT with the local problems solved by fixed number of CG steps.
     *
     * @param gamma the regularization parameter
     * @param maxIter max number of iterations
     * @return trainErrorArray the training error in each iteration
     * @return objValArray the objective values in each iteration
     * @return timeArray the elapsed times counted at each iteration
     */
    def train(gamma: Double, maxIter: Int): (Array[Double], Array[Double], Array[Double]) = {
        // setup the executors for training
        val rddTrain: RDD[Executor] = this.rdd
                                    .map(exe => {exe.setGamma(gamma);
                                                 exe})
                                    .persist()
        rddTrain.count
        val t0: Double = System.nanoTime()
        for (j <- 0 until this.d) this.w(j) = 0
        
        
        // setup buffers
        val ngamma = this.n * gamma
        val xy: Array[Double] = rddTrain.map(_.xy.toArray)
                                    .reduce((a,b) => (a,b).zipped.map(_ + _))
        val r: DenseVector[Double] = DenseVector.zeros[Double](this.d)
        val p: DenseVector[Double] = DenseVector.zeros[Double](this.d)
        val ap: DenseVector[Double] = DenseVector.zeros[Double](this.d)
        val wnew: DenseVector[Double] = new DenseVector(this.w)
        val xxw: DenseVector[Double] = new DenseVector(this.xxByP(this.w, rddTrain))
        r := (new DenseVector(xy)) - ngamma * wnew - xxw
        p := r
        var rsold: Double = r.toArray.map(x => x*x).sum
        var rsnew: Double = 0.0
        var alpha: Double = 0.0
        
        // record the objectives of each iteration
        val trainErrorArray: Array[Double] = new Array[Double](maxIter)
        val objValArray: Array[Double] = new Array[Double](maxIter)
        val timeArray: Array[Double] = new Array[Double](maxIter)
        
        var t1: Double = System.nanoTime()
        var i: Int = -1
        
        for (t <- 0 until maxIter) {
            var xxp: DenseVector[Double] = new DenseVector(this.xxByP(p.toArray, rddTrain))
            ap := ngamma * p + xxp
            var pap: Double = 0.0
            for (j <- 0 until d) pap += p(j) * ap(j)
            alpha = rsold / pap
            wnew += alpha * p
            r -= alpha * ap
            rsnew = r.toArray.map(a => a*a).sum
            
            if (t % 2 == 0) { // record the objective values and training errors
                i += 1
                timeArray(i) = (t1 - t0) * 1.0E-9
                t1 = System.nanoTime()
                this.objs(wnew.toArray, rddTrain)
                trainErrorArray(i) = this.trainError
                objValArray(i) = this.objVal
                if (!this.isMute) println("Iteration " + t.toString + ":\t objective value is " + this.objVal.toString + ",\t time: " + timeArray(t).toString)
            }
            if (rsnew < this.gNormTol) {
                return (trainErrorArray.slice(0, i+1), 
                        objValArray.slice(0, i+1), 
                        timeArray.slice(0, i+1))
            }
            p *= rsnew / rsold
            p += r
            rsold = rsnew
        }
        
        (trainErrorArray.slice(0, i+1), objValArray.slice(0, i+1), timeArray.slice(0, i+1))
    }
    
    /**
     * Compute X * X' * p, where X is d-by-n and p is d-by-1.
     */
    def xxByP(pArray: Array[Double], rddTrain: RDD[Executor]): Array[Double] = {
        val pBc: Broadcast[Array[Double]] = this.sc.broadcast(pArray)
        val xxp: Array[Double] = rddTrain.map(_.xxByP(pBc.value))
                                    .reduce((a,b) => (a,b).zipped.map(_ + _))
        xxp
    }

    /**
     * Compute the objective value and training error.
     */
    def objs(wArray: Array[Double], rddTrain: RDD[Executor]): Unit = {
        val wBc: Broadcast[Array[Double]] = this.sc.broadcast(wArray)
        val tmp: (Double, Double) = rddTrain.map(_.objs(wBc.value))
                                    .reduce((a,b) => (a._1+b._1, a._2+b._2))
        this.objVal = tmp._1 * this.nInv
        this.trainError = tmp._2 * this.nInv
    }
            
    /** 
     * Compute the mean squares error for test data.
     *
     * @param dataTest RDD of label-feature pair
     * @return mse of the test data
     */
    def predict(dataTest: RDD[(Double, Array[Double])]): Double = {
        val nTest: Long = dataTest.count
        val wBc: Broadcast[Array[Double]] = this.sc.broadcast(this.w)
        val error: Double = dataTest.map(pair => (pair._1, (pair._2, wBc.value).zipped.map(_ * _).sum))
                        .map(pair => (pair._1 - pair._2) * (pair._1 - pair._2))
                        .mean
        //val mse: Double = error / nTest.toDouble
        error
    }
}


/**
 * Perform local computations. 
 * 
 * @param arr array of (label, feature) pairs
 */
class Executor(arr: Array[(Double, Array[Double])]) {
    // get data
    val s: Int = arr.size
    val d: Int = arr(0)._2.size
    val y: DenseVector[Double] = new DenseVector(arr.map(pair => pair._1))
    val x: DenseMatrix[Double] = new DenseMatrix(d, s, arr.map(pair => pair._2).flatten)
    val xy: DenseVector[Double] = x * y

    val sDouble = this.s.toDouble

    // specific to training
    var gamma: Double = 0.0
    
    def setGamma(gamma0: Double): Unit = {
        this.gamma = gamma0
    }
    
    /**
     * Compute X * X' * p.
     * Here X is d-by-s and p is d-by-1.
     */
    def xxByP(pArray: Array[Double]): Array[Double] = {
        val p: DenseVector[Double] = new DenseVector(pArray)
        val xp: DenseVector[Double] = this.x.t * p
        (this.x * xp).toArray
    }
            
    /**
     * Compute the local objective value and training error.
     */
    def objs(wArray: Array[Double]): (Double, Double) = {
        val w: DenseVector[Double] = new DenseVector(wArray)
        // training error
        var res: DenseVector[Double] = this.x.t * w - this.y
        val trainError: Double = res.toArray.map(a => a*a).sum
        // objective function value
        val wNorm: Double = wArray.map(a => a*a).sum
        val objVal: Double = (trainError + this.sDouble * this.gamma * wNorm) / 2
        (objVal, trainError)
    }
            
    /**
     * Compute the local gradient.
     * As by-products, also compute the training error and objective value.
     *
     * @param w the current solution
     * @return g = X' * (X * w - y) + s * gamma * w , the local gradient
     * @return trainError = ||X w - y||_2^2 , the local training error
     * @return objVal = 0.5*||X w - y||_2^2 + 0.5*s*gamma*||w||_2^2 , the local objective function value
     */
    def grad(wArray: Array[Double]): (Array[Double], Double, Double) = {
        val w: DenseVector[Double] = new DenseVector(wArray)
        // gradient
        val sgamma = this.sDouble * this.gamma
        var res: DenseVector[Double] = this.x.t * w - this.y
        var g: DenseVector[Double] = this.x * res + sgamma * w
        // training error
        val trainError: Double = res.toArray.map(a => a*a).sum
        // objective function value
        val wNorm: Double = wArray.map(a => a*a).sum
        val objVal: Double = (trainError + sgamma * wNorm) / 2
        (g.toArray, trainError, objVal)
    }
}


