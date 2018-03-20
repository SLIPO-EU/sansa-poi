package eu.slipo.algorithms

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler


class oneHotEncoder {
  
  /*
     * One hot encoding categorical data
     * */
    def oneHotEncoding(poiCategories: RDD[(Int, Iterable[Int])], spark: SparkSession){
      // create a set to contain all categories
      var set = scala.collection.mutable.Set[Int]()
      // put all categories to set
      poiCategories.collect().foreach(x => x._2.foreach(y => set += y))
      // create multiple columns base on the length of set
      val numPOIS = poiCategories.count().toInt
      val categoryArray = set.toArray
      var oneHotMatrix = Array.ofDim[Int](numPOIS, categoryArray.length)
      // initialize distance matrix
      for (i <- 0 to numPOIS-1) {
         for ( j <- 0 to categoryArray.length-1) {
            oneHotMatrix(i)(j) = 0
         }
      }
      // create one hot encoded matrix, row by row
      var count = 0
      poiCategories.collect().foreach(x => {x._2.foreach(y => {oneHotMatrix(count)(categoryArray.indexOf(y)) = 1}); count += 1})
      
      // vector keep all StructField
      var fields = Array.ofDim[StructField](categoryArray.length)
       var featureColumns = Array.ofDim[String](categoryArray.length)
      for (i <- 0 to categoryArray.length-1){
        fields(i) = new StructField(i.toString(), IntegerType, true)
        featureColumns(i) = i.toString()
      }
      var schema = new StructType(fields)
      println(categoryArray.length)
      println(fields.length)
      println(schema.fields.length)  // TODO no idea why the length of schema is 0
      val oneHotEncodedRDD = spark.sparkContext.parallelize(oneHotMatrix).map(x => Row(x.toList))
      val oneHotEncodedDF = spark.createDataFrame(oneHotEncodedRDD, schema)
      // set up 'features' column
      val assembler = (new VectorAssembler().setInputCols(featureColumns).setOutputCol("features"))
      val featureData = assembler.transform(oneHotEncodedDF)
//      val kmeans = new KMeans().setK(2).setSeed(1L).setFeaturesCol("features").setPredictionCol("prediction")
//      val model = kmeans.fit(featureData)
//      
//      println("Cluster Centers: ")
//      model.clusterCenters.foreach(println)
    }
}