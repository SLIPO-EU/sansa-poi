package eu.slipo.poi

import java.net.URI
import java.io.PrintWriter
import java.util.Calendar

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleRDD
import net.sansa_stack.rdf.spark.analytics
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.jena.graph.Triple
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.mllib.clustering.dbscan.DBSCAN
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import smile.mds.MDS
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.feature.VectorAssembler



object poiClustering {
  
    val dataSource = "resources/data/herold_pois_austria_v0.3.nt"  // there are 312385 pois
    val termValueUri = "http://slipo.eu/def#termValue"
    val termPrefix = "http://slipo.eu/id/term/" 
    val typePOI = "http://slipo.eu/def#POI"
    val categoryPOI = "http://slipo.eu/def#category"
    val termPOI = "http://slipo.eu/def#termValue"
    val poiPrefix = "http://slipo.eu/id/poi/"
    val categoriesFile = "resources/results/categories"
    val results = "resources/results/clustering_result.txt"
    val poiCategoriesFile = "resources/results/poi_categories"
    val runTimeStatics = "resources/results/runtime.txt"
    val now = Calendar.getInstance()
    val fileWriter = new PrintWriter(results)
    val runTimeSta = new PrintWriter(runTimeStatics)
    
    /*
     * Jaccard Similarity Coefficient between two sets of categories corresponding to two pois
     * */
    def jaccardSimilarity(x: Iterable[Int], y: Iterable[Int]): Double = {
      val x_ = x.toSet
      val y_ = y.toSet
      val union_l = x_.toList.length + y_.toList.length
      val intersect_l = x_.intersect(y_).toList.length
      intersect_l / (union_l - intersect_l)
    }
    
    
    /*
     * Write (category_id, set(category_values)) to file
     * */
    def getCategoryValues(sparkSession: SparkSession, data: RDD[Triple]): RDD[(Int, Set[String])] = {
      // find all categories by id(for category aggregation)
      val categoriesValue = data.filter(x => x.getPredicate.toString().equalsIgnoreCase(termValueUri))
      // get category id and it's corresponding value
      val categoriesIdValue = categoriesValue.map(x => (x.getSubject.toString().replace(termPrefix, "").toInt, x.getObject.toString()))
      // group by id and put all values of category to a set
      categoriesIdValue.groupByKey().sortByKey().map(x => (x._1, x._2.toSet))
    }
    
   
    /*
     * Write clustering results to file
     * */
    def writeClusteringResult(clusters: Map[Int, Array[Long]], categories: RDD[(Int, Set[String])], poiCategories: RDD[(Int, Iterable[Int])]) = {
      val assignments = clusters.toList.sortBy { case (k, v) => v.length }
      val assignmentsStr = assignments.map { case (k, v) => s"$k -> ${v.sorted.mkString("[", ",", "]")}, ${v.map(poi => poiCategories.lookup(poi.toInt).head.mkString("(", "," ,")")).mkString("[", ",", "]")}, ${v.map(poi => poiCategories.lookup(poi.toInt).head.map(category => categories.lookup(category).mkString(",")).mkString("(", ",", ")")).mkString("[", ",", "]")}"}.mkString("\n")
      val sizesStr = assignments.map {_._2.length}.sorted.mkString("(", ",", ")")
      fileWriter.println(s"Cluster assignments:\n $assignmentsStr\n")
      fileWriter.println(s"cluster sizes:\n $sizesStr\n")
    }
    
    
    /*
     * Spectral clustering
     * */
    def piClustering(pairwisePOISimilarity: RDD[(Long, Long, Double)], sparkSession: SparkSession, dataRDD: RDD[Triple], poiCategories: RDD[(Int, Iterable[Int])]) = {
      val model = new PowerIterationClustering().setK(3).setMaxIterations(1).setInitializationMode("degree").run(pairwisePOISimilarity)
      val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
      // get categories and clustering result
      writeClusteringResult(clusters, getCategoryValues(sparkSession, dataRDD), poiCategories)
    }
    
    
    /*
     * Kmeans clustering based on given coordinates
     * */
    def kmeansClustering(coordinates: Array[(Double, Double)], spark: SparkSession, numClusters: Int) = {
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
      
      val kmeans = new KMeans().setK(numClusters).setSeed(1L)
      val model = kmeans.fit(featureData)
      
      println("Cluster Centers: ")
      model.clusterCenters.foreach(println)
    }
    
    /*
     * DBSCAN
     * */
    def dbscanClustering(coordinates: Array[(Double, Double)], spark: SparkSession) = {
      // data, eps, minPoints, maxPoints
      val coordinatesVector = coordinates.map(x => Vectors.dense(x._1, x._2))
      val coordinatesRDD = spark.sparkContext.parallelize(coordinatesVector)
      val model = DBSCAN.train(coordinatesRDD, 0.1, 1, 10)
      model.labeledPoints.map(p =>  s"${p.x},${p.y},${p.cluster}").saveAsTextFile("resources/results/dbscan.txt")
    }
    
    /*
     * Multi-dimensional scaling
     * */
    def multiDimensionScaling(distancePairs: RDD[(Long, Long, Double)], numPOIS: Int): Array[Array[Double]] = {
      // vector keep recorded poi
      var vector = Array.ofDim[Long](numPOIS)
      // positive symmetric distance matrix
      var distanceMatrix = Array.ofDim[Double](numPOIS, numPOIS)
      // initialize distance matrix
      for (i <- 0 to numPOIS-1) {
         vector(i) = 0
         for ( j <- 0 to numPOIS-1) {
            distanceMatrix(i)(j) = 0.0
         }
      }
      var i = 0
      distancePairs.collect().foreach(x => {
                                          if(!vector.contains(x._1)){ // if there is no record for this poi
                                            vector(i) = x._1
                                            i += 1
                                          }
                                          if(!vector.contains(x._2)){ // if there is no record for this poi
                                            vector(i) = x._2
                                            i += 1
                                          }
                                          val i1 = vector.indexOf(x._1) // get the index as x-y axis for matrix
                                          val i2 = vector.indexOf(x._2) // get the index as x-y axis for matrix
                                          distanceMatrix(i1)(i2) = x._3;
                                          distanceMatrix(i2)(i1) = x._3;
                                          println(x._3)
                                          })
      // create coordinates
      val mds = new MDS(distanceMatrix, 2, true)
      mds.getCoordinates
    }
    
    
    def main(args: Array[String]){
      
      val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader resources/data/tomtom_pois_austria_v0.3.nt")
      .getOrCreate()
      spark.conf.set("spark.executor.memory", "6g")
      spark.conf.set("spark.driver.memory", "6g")
      
      // read NTriple file, get RDD contains triples
      val dataRDD = NTripleReader.load(spark, dataSource)
      
      // find all the categories of pois
      val poiFlatCategories = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(categoryPOI))
      
      // from 'Node' to string, and remove common prefix
      val poiRawCategories = poiFlatCategories.map(x => (x.getSubject.toString().replace(poiPrefix, "").toInt, x.getObject.toString().replace(termPrefix, "").toInt))
      
      // get the categories for each poi TODO not encourage to use groupByKey as it is slow for large dataset, sample 1% to reduce the computation costs
      val poiCategories = poiRawCategories.groupByKey().sample(false, 0.0001, 0)
      
      val numberPOIs = poiCategories.count().toString().toInt
      // get the number of pois, and save corresponding categories
      fileWriter.println(s"Number of POIs: ${poiCategories.count().toString()}")
      
      // considering PIC https://spark.apache.org/docs/1.5.1/mllib-clustering.html, build ((sid, ()), (did, ())) RDD
      val pairwisePOICategories = poiCategories.cartesian(poiCategories).filter{ case (a, b) => a._1.toInt < b._1.toInt }
      
      // from ((sid, ()), (did, ())) to (sid, did, similarity)
      val pairwisePOISimilarity = pairwisePOICategories.map(x => (x._1._1.toString().toLong, x._2._1.toString().toLong, jaccardSimilarity(x._1._2, x._2._2)))
      
      // distance RDD
      val distancePairs = pairwisePOISimilarity.map(x => (x._1, x._2, 1.0 - x._3))
      val coordinates = multiDimensionScaling(distancePairs, numberPOIs).map(x => (x(0), x(1)))
      
      // kmeans clustering, number of clusters 2
      kmeansClustering(coordinates, spark, 2)
      
      // dbscan clustering, TODO solve scala version flicts with SANSA
      // dbscanClustering(coordinates, spark)
      
      // run pic, 50 centroids and 5 iterations
      //piClustering(pairwisePOISimilarity, spark, dataRDD, poiCategories)
      
      // stop spark session
      spark.stop()
    }
}