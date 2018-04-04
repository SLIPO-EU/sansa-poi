package eu.slipo.algorithms

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler


class Kmeans {
  
    /*
     * K-means clustering based on given coordinates
     * */
    def kmeansClustering(coordinates: Array[(Long, Double, Double)], numClusters: Int, spark: SparkSession) = {
      // create schema
      val schema = StructType(
            Array(
              StructField("id", LongType, true),
              StructField("c1", DoubleType, true),
              StructField("c2", DoubleType, true)
            )
      )
      val coordinatesRDD = spark.sparkContext.parallelize(coordinates.toSeq).map(x => Row(x._1, x._2, x._3))
      val coordinatesDF = spark.createDataFrame(coordinatesRDD, schema)
      val assembler = new VectorAssembler().setInputCols(Array("c1", "c2")).setOutputCol("features")
      val featureData = assembler.transform(coordinatesDF)
      
      val kmeans = new KMeans().setK(numClusters).setSeed(1L).setFeaturesCol("features").setPredictionCol("prediction")
      val model = kmeans.fit(featureData)
      val transformedDataFrame = model.transform(featureData)
      import spark.implicits._
      // get (cluster_id, poi_id)
      val clusterIdPoi = transformedDataFrame.map(f => (f.getInt(f.size-1), f.getLong(0))).rdd.groupByKey()
      val clustersMDSKM = clusterIdPoi.map(x => (x._1, x._2.toArray)).collectAsMap().toMap
      clustersMDSKM
    }
}