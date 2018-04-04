package eu.slipo.poi

import java.io.PrintWriter

import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.jena.graph.Triple
import eu.slipo.algorithms.{Distances, PIC, multiDS, oneHotEncoder, Kmeans}
import eu.slipo.datatypes.Cluster
import eu.slipo.datatypes.Clusters
import eu.slipo.datatypes.Coordinate
import eu.slipo.datatypes.Poi
import eu.slipo.datatypes.Categories
import org.apache.spark.ml.clustering.KMeans
import org.json4s._
import org.json4s.jackson.Serialization



object poiClustering {
  
    val dataSource = "src/main/resources/data/tomtom_pois_austria_v0.3.nt"  // there are 312385 pois for tomtom and 350053 for herold
    val termValueUri = "http://slipo.eu/def#termValue"
    val termPrefix = "http://slipo.eu/id/term/" 
    val typePOI = "http://slipo.eu/def#POI"
    val coordinatesPredicate = "http://www.opengis.net/ont/geosparql#asWKT"
    val categoryPOI = "http://slipo.eu/def#category"
    val termPOI = "http://slipo.eu/def#termValue"
    val poiPrefix = "http://slipo.eu/id/poi/"
    val categoriesFile = "src/main/results/categories"
    val pic_results = "src/main/resources/results/pic_clusters.json"
    val kmeans_results = "src/main/resources/results/kmeans_clusters.json"
    val mds_kmeans_results = "src/main/resources/results/mds_kmeans_clusters.json"
    val poiCategoriesFile = "src/main/resources/results/poi_categories"
    val runTimeStatics = "src/main/resources/results/runtime.txt"
    val picFileWriter = new PrintWriter(pic_results)
    val kmeansFileWriter = new PrintWriter(kmeans_results)
    val mdsKMFileWriter = new PrintWriter(mds_kmeans_results)
        
    /*
     * get (category_id, category_values_set)
     * */
    def getCategoryValues(sparkSession: SparkSession, data: RDD[Triple]): RDD[(Long, Categories)] = {
      // get category id(s)
      val categoryIds = data.filter(x => x.getPredicate.toString().equalsIgnoreCase(termValueUri))
      // get category id and it's corresponding values
      val categoriesIdValues = categoryIds.map(x => (x.getSubject.toString().replace(termPrefix, "").toLong, x.getObject.toString().replaceAll("\"", "")))
      // group by id and put all values of category to a set
      categoriesIdValues.groupByKey().map(x => (x._1, Categories(scala.collection.mutable.Set(x._2.toList:_*))))
    }
    
    /*
     * Write clustering results to file
     * */
    def writeClusteringResult(clusters: Map[Int, Array[Long]], pois: RDD[Poi], fileWriter: PrintWriter) = {
      val assignments = clusters.toList.sortBy { case (k, v) => v.length }
      val poisKeyPair = pois.keyBy(f => f.poi_id)
      val clustersPois = Clusters(assignments.map(f => Cluster(f._1, f._2.map(x => poisKeyPair.lookup(x.toInt).head))))
      implicit val formats = DefaultFormats
      Serialization.writePretty(clustersPois, fileWriter)
    }

    /*
    * Build a list of Poi objects
    * */
    def generatePois(sparkSession: SparkSession, poiCoordinates: RDD[(Long, Coordinate)], categoryIdValues: RDD[(Long, Categories)], poiCategoryIds: RDD[(Long, Set[Long])]): RDD[Poi] = {
      val categoriesMap = categoryIdValues.collectAsMap()
      val poiCategoryIdsMap = poiCategoryIds.collectAsMap()
      poiCoordinates.map(f => Poi(f._1, f._2,
                                  {val categories = Categories(scala.collection.mutable.Set[String]())
                                  poiCategoryIdsMap(f._1).foreach(x => categories.categories ++=
                                                                       { // some of the category id does not have corresponding category value
                                                                         if (categoriesMap.contains(x))
                                                                          {
                                                                            categoriesMap(x).categories
                                                                          }
                                                                        else {
                                                                            scala.collection.mutable.Set[String](s"unknown for category id : $x")
                                                                          }
                                                                       }
                                  )
                                  categories})).persist()
    }
    
    
    def main(args: Array[String]){
      System.setProperty("hadoop.home.dir", "C:\\Hadoop")
      val spark = SparkSession.builder
                  .master("local[*]")
                  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                  .appName("Triple reader resources/data/tomtom_pois_austria_v0.3.nt")
                  .getOrCreate()
      spark.conf.set("spark.executor.memory", "10g")
      spark.conf.set("spark.driver.memory", "10g")

      // read NTriple file, get RDD contains triples
      val dataRDD = NTripleReader.load(spark, dataSource)
      
      // get the coordinates of pois
      val pattern = "POINT(.+ .+)".r
      val poiCoordinates = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(coordinatesPredicate))
                                    .map(x => (x.getSubject.toString().replace(poiPrefix, "").replace("/geometry", "").toLong,
                                               pattern.findFirstIn(x.getObject.toString()).head.replace("POINT", "")
                                                 .replace("^^http://www.opengis.net/ont/geosparql#wktLiteral", "").replaceAll("^\"|\"$", "")))

      // transform to Coordinate object
      val poiCleanCoordinates = poiCoordinates.mapValues(x => {val coordinates = x.replace("(", "").replace(")", "").split(" ")
                                    Coordinate(coordinates(0).toDouble, coordinates(1).toDouble)})
      
      // find pois in vinna, 72549 in total for herold, sample 0.1%
      val poiVinna = poiCleanCoordinates.filter(x => (x._2.longitude>=16.192851 && x._2.longitude<=16.593533)
                                                      && (x._2.latitude>=48.104194 && x._2.latitude<=48.316388)
                                                ).sample(withReplacement = false, fraction = 0.002, seed = 0).persist()
      val keys = poiVinna.keys.collect()
      println(s"Number of sampled poi in Vinna: ${keys.length}")

      // find all the categories of pois in Vinna
      val poiFlatCategories = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(categoryPOI))
      val poiCategoriesVinna = poiFlatCategories.filter(x => keys.contains(x.getSubject.toString().replace(poiPrefix, "").toLong))

      // from 'Node' to (poi_id, category_id) pairs, possible with duplicated keys
      val poiRawCategoriesVinna = poiCategoriesVinna.map(x => (x.getSubject.toString().replace(poiPrefix, "").toLong,
                                                         x.getObject.toString().replace(termPrefix, "").toLong))
      
      // get the categories for each poi in Vinna
      val poiCategorySetVinna = poiRawCategoriesVinna.groupByKey().map(f => (f._1, f._2.toSet)).persist()
      println(s"Number of sampled poi in Vinna, with categories: ${poiCategorySetVinna.count()}")
      // aggregate category values based on category id
      val categoryIdValues = getCategoryValues(spark, dataRDD).persist()
      println(s"Number of categories: ${categoryIdValues.count()}")
      val poiVinnaCategoryIds = poiCategorySetVinna.flatMap(f => f._2).collect().toSet
      println(s"Number of categories in Vinna: ${poiVinnaCategoryIds.size}")
      val categoryVinnaIdValues = categoryIdValues.filter(f => poiVinnaCategoryIds.contains(f._1)).persist()
      println(s"Number of categories with value in Vinna: ${categoryVinnaIdValues.count()}")
      val pois = generatePois(spark, poiVinna, categoryVinnaIdValues, poiCategorySetVinna).persist()
      println(s"number of poi: ${pois.count()}")

      // one hot encoding
      val df = new oneHotEncoder().oneHotEncoding(poiCategorySetVinna, spark)

      // kmeans
      val kmeans = new KMeans().setK(10).setSeed(1L).setFeaturesCol("features").setPredictionCol("prediction")
      val model = kmeans.fit(df)
      val transformedDataFrame = model.transform(df)
      import spark.implicits._
      // get (cluster_id, poi_id)
      val clusterIdPoi = transformedDataFrame.map(f => (f.getInt(f.size-1), f.getInt(0).toLong)).rdd.groupByKey()
      val clustersKM = clusterIdPoi.map(x => (x._1, x._2.toArray)).collectAsMap().toMap
      writeClusteringResult(clustersKM, pois, kmeansFileWriter)

      // https://spark.apache.org/docs/1.5.1/mllib-clustering.html, build ((sid, ()), (did, ())) RDD
      val pairwisePOICategorySet = poiCategorySetVinna.cartesian(poiCategorySetVinna).filter{ case (a, b) => a._1 < b._1 }
      // from ((sid, ()), (did, ())) to (sid, did, similarity)
      val pairwisePOISimilarity = pairwisePOICategorySet.map(x => (x._1._1.toLong, x._2._1.toLong,
                                                                  new Distances().jaccardSimilarity(x._1._2, x._2._2))).persist()
      // pic clustering
      val clustersPIC = new PIC().picSparkML(pairwisePOISimilarity, 15, 5, spark)
      writeClusteringResult(clustersPIC, pois, picFileWriter)

      // distance RDD, from (sid, did, similarity) to (sid, did, distance)
      val distancePairs = pairwisePOISimilarity.map(x => (x._1, x._2, 1.0 - x._3)).persist()
      
      // generate coordindates in 2 dimension, and run kmeans
      val poi2Coordinates = new multiDS().multiDimensionScaling(distancePairs, poiCategorySetVinna.count().toInt, 2)
      val clustersMDSKM = new Kmeans().kmeansClustering(poi2Coordinates, 15, spark)
      writeClusteringResult(clustersMDSKM, pois, mdsKMFileWriter)

      // dbscan clustering, TODO solve scala version flicts with SANSA
      // dbscanClustering(coordinates, spark)

      // stop spark session
      picFileWriter.close()
      kmeansFileWriter.close()
      mdsKMFileWriter.close()
      spark.stop()
    }
}