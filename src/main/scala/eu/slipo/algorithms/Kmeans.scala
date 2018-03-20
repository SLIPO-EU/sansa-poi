package eu.slipo.algorithms

import org.apache.spark.ml.clustering.KMeans

class kmeans {
  
    /*
     * K-means clustering based on given coordinates
     * */
    def kmeansClustering(coordinates: Array[(Double, Double)], numClusters: Int, spark: SparkSession) = {
      // create schema
      val schema = StructType(
            Array(
            StructField("c1", DoubleType, true), 
            StructField("c2", DoubleType, true)
            )
      )
      val coordinatesRDD = spark.sparkContext.parallelize(coordinates.toSeq).map(x => Row(x._1, x._2))
      val coordinatesDF = spark.createDataFrame(coordinatesRDD, schema)
      val assembler = (new VectorAssembler().setInputCols(Array("c1", "c2")).setOutputCol("features"))
      val featureData = assembler.transform(coordinatesDF)
      
      val kmeans = new KMeans().setK(numClusters).setSeed(1L).setFeaturesCol("features").setPredictionCol("prediction")
      val model = kmeans.fit(featureData)
      
      // println("Cluster Centers: ")
      // model.clusterCenters.foreach(println)
      
      model.getPredictionCol
      
    }
}