package eu.slipo.poi

import java.io.PrintWriter

import org.apache.spark.sql._
import eu.slipo.algorithms.{Distances, Encoder, Kmeans, PIC}
import eu.slipo.datatypes._
import eu.slipo.utils.tomTomDataProcessing
import eu.slipo.utils.Common
import org.json4s._
import org.json4s.native.JsonMethods.parse
import org.json4s.jackson.Serialization

object poiClustering {


  /**
   * main function
   */
  def main(args: Array[String]) {
    implicit val formats = DefaultFormats
    val raw_conf = scala.io.Source.fromFile("src/main/resources/conf.json").reader()
    val conf = parse(raw_conf).extract[appConfig]
    val profileWriter = new PrintWriter(conf.clustering.profile)
    val picFileWriter = new PrintWriter(conf.clustering.pic)
    val oneHotKMFileWriter = new PrintWriter(conf.clustering.oneHotKM)
    val mdsKMFileWriter = new PrintWriter(conf.clustering.mdsKM)
    val word2VecKMFileWriter = new PrintWriter(conf.clustering.word2VecKM)
    val picDistanceMatrixWriter = new PrintWriter(conf.clustering.picDistanceMatrix)
    val mdsCoordinatesWriter = new PrintWriter(conf.clustering.mdsCoordinates)

    // System.setProperty("hadoop.home.dir", "C:\\Hadoop") // for Windows system
    val spark = SparkSession.builder
      .master(conf.spark.master)
      .config("spark.serializer", conf.spark.spark_serializer)
      .config("spark.executor.memory", conf.spark.spark_executor_memory)
      .config("spark.driver.memory", conf.spark.spark_driver_memory)
      .config("spark.driver.maxResultSize", conf.spark.spark_driver_maxResultSize)
      .appName(conf.spark.app_name)
      .getOrCreate()

    val t0 = System.nanoTime()
    val tomTomData = new tomTomDataProcessing(spark = spark, conf = conf)
    val pois = tomTomData.pois
    val poiCategorySetVienna = tomTomData.poiCategoryId
    val t1 = System.nanoTime()
    profileWriter.println("Elapsed time preparing data: " + (t1 - t0)/1000000000 + "s")

    // one hot encoding
    val oneHotDF = new Encoder().oneHotEncoding(poiCategorySetVienna, spark)
    val oneHotClusters = new Kmeans().kmClustering(numClusters = 10, df = oneHotDF, spark = spark)
    Common.writeClusteringResult(spark.sparkContext, oneHotClusters, pois, oneHotKMFileWriter)
    val t2 = System.nanoTime()
    profileWriter.println("Elapsed time one hot: " + (t2 - t0)/1000000000 + "s")

    // word2Vec encoding
    val avgVectorDF = new Encoder().wordVectorEncoder(poiCategorySetVienna, spark)
    val avgVectorClusters = new Kmeans().kmClustering(numClusters = 10, df = avgVectorDF, spark = spark)
    Common.writeClusteringResult(spark.sparkContext, avgVectorClusters, pois, word2VecKMFileWriter)
    val t3 = System.nanoTime()
    profileWriter.println("Elapsed time word2Vec: " + (t3 - t0)/1000000000 + "s")

    // pic clustering, build ((sid, ()), (did, ())) RDD
    val pairwisePOICategorySet = poiCategorySetVienna.cartesian(poiCategorySetVienna).filter { case (a, b) => a._1 < b._1 }
    // from ((sid, ()), (did, ())) to (sid, did, similarity)
    val pairwisePOISimilarity = pairwisePOICategorySet.map(x => (x._1._1.toLong, x._2._1.toLong,
      new Distances().jaccardSimilarity(x._1._2, x._2._2))).persist()
    val picDistanceMatrix = DistanceMatrix(pairwisePOISimilarity.map(x => Distance(x._1, x._2, 1-x._3)).collect().toList)
    Serialization.writePretty(picDistanceMatrix, picDistanceMatrixWriter)
    val clustersPIC = new PIC().picSparkML(pairwisePOISimilarity, 10, 5, spark)
    Common.writeClusteringResult(spark.sparkContext, clustersPIC, pois, picFileWriter)
    val t4 = System.nanoTime()
    profileWriter.println("Elapsed time cartesian: " + (t4 - t0)/1000000000 + "s")

    //// distance RDD, from (sid, did, similarity) to (sid, did, distance)
    val distancePairs = pairwisePOISimilarity.map(x => (x._1, x._2, 1.0 - x._3)).persist()
    val (mdsDF, coordinates) = new Encoder().mdsEncoding(distancePairs = distancePairs, poiCategorySetVienna.count().toInt, dimension = 3, spark = spark)
    val mdsCoordinates = MdsCoordinates(coordinates.map(f=> MdsCoordinate(f._1, f._2)))
    Serialization.writePretty(mdsCoordinates, mdsCoordinatesWriter)
    val mdsClusters = new Kmeans().kmClustering(numClusters = 10, df = mdsDF, spark = spark)
    Common.writeClusteringResult(spark.sparkContext, mdsClusters, pois, mdsKMFileWriter)
    val t5 = System.nanoTime()
    profileWriter.println("Elapsed time mds: " + (t5 - t0)/1000000000 + "s")

    // dbscan clustering, TODO solve scala version flicts with SANSA
    // dbscanClustering(coordinates, spark)
    // stop spark session
    // viennaTriplesWriter.close()
    picFileWriter.close()
    oneHotKMFileWriter.close()
    mdsKMFileWriter.close()
    word2VecKMFileWriter.close()
    profileWriter.close()
    picDistanceMatrixWriter.close()
    mdsCoordinatesWriter.close()
    spark.stop()
  }
}
