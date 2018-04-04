package eu.slipo.algorithms

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler


class oneHotEncoder {
  
  /*
     * One hot encoding categorical data
     * */
    def oneHotEncoding(poiCategories: RDD[(Long, Set[Long])], spark: SparkSession): DataFrame = {
      // create a set to contain all categories
      var set = scala.collection.mutable.Set[Long]()
      // put all categories to set
      poiCategories.collect().foreach(x => x._2.foreach(y => set += y))
      // create columns base on the length of set
      val numPOIS = poiCategories.count().toInt  // Array.ofDim only accept Int
      val categoryArray = set.toArray
      val oneHotMatrix = Array.ofDim[Int](numPOIS, categoryArray.length + 1) // one column keep poi id

      // initialize distance matrix, collect first needed
      var i = 0
      poiCategories.collect().foreach(x =>
        {
          oneHotMatrix(i)(0) = x._1.toInt
          for ( j <- 1 until categoryArray.length + 1) {
            oneHotMatrix(i)(j) = 0
          }
          x._2.foreach(y =>
            { // encode corresponding category value to 1
              oneHotMatrix(i)(categoryArray.indexOf(y) + 1) = 1
            }
          )
          i += 1
        }
      )

      // vector keep all StructField
      val fields = Array.ofDim[StructField](categoryArray.length + 1)
      val featureColumns = Array.ofDim[String](categoryArray.length + 1)
      // keep other columns with integer type
      for (i <- 0 until categoryArray.length + 1){
        fields(i) = StructField(i.toString, IntegerType, true)
        featureColumns(i) = i.toString
      }
      val schema = new StructType(fields)
      val oneHotEncodedRDD = spark.sparkContext.parallelize(oneHotMatrix).map(x => Row.fromSeq(x.toList))
      val oneHotEncodedDF = spark.createDataFrame(oneHotEncodedRDD, schema)
      // set up 'features' column
      val assemblerFeatures = new VectorAssembler().setInputCols(featureColumns.slice(1, featureColumns.length)).setOutputCol("features")
      val transformedDf = assemblerFeatures.transform(oneHotEncodedDF)
      transformedDf
    }
}