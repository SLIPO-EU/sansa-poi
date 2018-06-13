package eu.slipo.algorithms

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.dbscan.DBSCAN

class dbSCAN {

  /*
     * DBSCAN
     * */
  def dbscanClustering(coordinates: Array[(Double, Double)], spark: SparkSession) = {
    val coordinatesVector = coordinates.map(x => Vectors.dense(x._1, x._2))
    val coordinatesRDD = spark.sparkContext.parallelize(coordinatesVector)
    // data, eps, minPoints, maxPoints
    val model = DBSCAN.train(coordinatesRDD, 0.1, 1, 10)
    model.labeledPoints.map(p => s"${p.x},${p.y},${p.cluster}").saveAsTextFile("resources/results/dbscan.txt")
  }
}