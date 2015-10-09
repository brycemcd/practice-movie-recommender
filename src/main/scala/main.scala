object Main {
  import java.io.File
  import scala.io.Source
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
  import org.apache.spark.mllib.linalg.Vectors

  import org.apache.spark.rdd._

  import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

  val conf = new SparkConf()
    .setAppName("ml try")
    .setMaster("local[6]")
    .set("spark.executor.memory", "30g")
    .set("spark.executor-memory", "30g")
    .set("spark.driver.memory", "30g")
    .set("spark.driver-memory", "30g")
    .set("spark.storage.memoryFraction", "0.9999")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "/home/brycemcd/Downloads")
  val sc = new SparkContext(conf)


  def mainTest(args:Array[String]) = {
    println("----")
    for(c <- conf.getAll) {
      println(c)
    }
    println(conf.get("spark.executor.memory"))
    println(sc)
    println("-----")
    sc.stop()
  }

  val dataDir = "/home/brycemcd/Cricket/mlib/"
  val movieLensHomeDir = dataDir + "data/"
  def buildRatingsTuple() = {
    println("crunching ratings")
    val ratings = sc.textFile(new File(movieLensHomeDir, "ratings.dat").toString).map { line =>
      val fields = line.split("::")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
    val numRatings = ratings.count
    val numUsers = ratings.map(_._2.user).distinct.count
    val numMovies = ratings.map(_._2.product).distinct.count

    println("Got " + numRatings + " ratings from "
      + numUsers + " users on " + numMovies + " movies.")
    ratings
  }

  def buildMoviesTuple() = {
    println("crunching movies")
    val movies = sc.textFile(new File(movieLensHomeDir, "movies.dat").toString).map { line =>
      val fields = line.split("::")
      //format: (movieId, movieName)
      (fields(0).toInt, fields(1))
    }
    movies
  }

  def partitionData(ratings: RDD[(Long, Rating)],
    myRatingsRDD: RDD[Rating]) : Tuple3[RDD[Rating], RDD[Rating], RDD[Rating]]  = {
    val numPartitions = 4
    // NOTE: caching these values results in out of heap errors. Not caching them
    // is not very expensive
    val training = ratings.filter(x => x._1 < 6)
      .values
      .union(myRatingsRDD)
      .repartition(numPartitions)
      //.cache()
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .repartition(numPartitions)
      ////.cache()
    val test = ratings.filter(x => x._1 >= 8).values //.cache()

    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)
    (training, validation, test)
  }

  def trainModel(dataSets:Tuple3[RDD[Rating], RDD[Rating], RDD[Rating]]) = {

    val training = dataSets._1
    val validation = dataSets._2
    val test = dataSets._3

    // training using ALS
    val ranks = List(1)
    val lambdas = List(0.01)
    val numIters = List(10)

    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1

    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      // bug?
      //val validationRmse = computeRmse(model, validation, numValidation)
      val validationRmse = computeRmse(model, validation, true)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    val testRmse = computeRmse(bestModel.get, test, true)

    println("The best model has a testRMSE of " + testRmse)
    bestModel
  }

  def printRecommendations(bestModel: Option[MatrixFactorizationModel], candidates: RDD[(Int, String)] )(implicit movies: RDD[(Int, String)]) = {
    val recommendations = bestModel.get
      .predict(candidates.map(candidate => (0, candidate._1)))
      .collect()
      .sortBy(- _.rating)
      .take(10)

    var i = 1
    val moviesArr = movies.toArray()
    println("Movies recommended for you:")
    recommendations.foreach { r =>
      val withMovie = movies.filter(_._1 == r.product).take(1)
      println(r)
      println("%2d".format(i) + ": " + withMovie(0)._1 + " - " + withMovie(0)._2)
      i += 1
    }
  }

  def main(args:Array[String]) = {
    val data = sc.textFile(dataDir + "data/collab_filter.txt")

    val movies = buildMoviesTuple()
    val ratings = buildRatingsTuple()
    val myRatings = loadRatings("data/personalRatings.txt")
    val myRatingsRDD = sc.parallelize(myRatings)

    val dataPartitions = partitionData(ratings, myRatingsRDD)

    val bestModel = trainModel(dataPartitions)

    val myRatedMovieIds = myRatings.map(_.product).toSet
    val candidates = movies.filter(movie => !myRatedMovieIds.contains(movie._1))
    printRecommendations(bestModel, candidates)(movies)
    sc.stop()
  }

  def computeRmse(model: MatrixFactorizationModel,
                  data: RDD[Rating],
                  implicitPrefs: Boolean) = {

    def mapPredictedRating(r: Double) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))

    val predictionsAndRatings = predictions.map{ x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

  def loadRatings(path: String): Seq[Rating] = {
      val lines = Source.fromFile(path).getLines()
      val ratings = lines.map { line =>
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0)
    if (ratings.isEmpty) {
      sys.error("No ratings provided.")
    } else {
      ratings.toSeq
    }
  }



}
