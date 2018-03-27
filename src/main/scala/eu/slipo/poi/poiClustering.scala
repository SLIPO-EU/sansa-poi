package eu.slipo.poi

import java.io.PrintWriter

import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.jena.graph.Triple
import eu.slipo.algorithms.Distances
import eu.slipo.algorithms.PIC
import eu.slipo.datatypes.Cluster
import eu.slipo.datatypes.Clusters
import eu.slipo.datatypes.Coordinate
import eu.slipo.datatypes.Poi
import eu.slipo.datatypes.Categories
import org.json4s._
import org.json4s.jackson.Serialization



object poiClustering {
  
    val dataSource = "resources/data/tomtom_pois_austria_v0.3.nt"  // there are 312385 pois for tomtom and 350053 for herold
    val termValueUri = "http://slipo.eu/def#termValue"
    val termPrefix = "http://slipo.eu/id/term/" 
    val typePOI = "http://slipo.eu/def#POI"
    val coordinatesPredicate = "http://www.opengis.net/ont/geosparql#asWKT"
    val categoryPOI = "http://slipo.eu/def#category"
    val termPOI = "http://slipo.eu/def#termValue"
    val poiPrefix = "http://slipo.eu/id/poi/"
    val categoriesFile = "resources/results/categories"
    val results = "resources/results/clusters.json"
    val poiCategoriesFile = "resources/results/poi_categories"
    val runTimeStatics = "resources/results/runtime.txt"
    val fileWriter = new PrintWriter(results)
        
    /*
     * Write (category_id, category_values_set) to file
     * */
    def getCategoryValues(sparkSession: SparkSession, data: RDD[Triple]): RDD[(Long, Categories)] = {
      // get category id(s)
      val categoryIds = data.filter(x => x.getPredicate.toString().equalsIgnoreCase(termValueUri))
      // get category id and it's corresponding values
      val categoriesIdValues = categoryIds.map(x => (x.getSubject.toString().replace(termPrefix, "").toLong, x.getObject.toString().stripMargin))
      // group by id and put all values of category to a set
      categoriesIdValues.groupByKey().map(x => (x._1, Categories(scala.collection.mutable.Set(x._2.toList:_*))))
    }
    
    /*
     * Write clustering results to file
     * */
    def writeClusteringResult(clusters: Map[Int, Array[Long]], pois: RDD[Poi]) = {
      val assignments = clusters.toList.sortBy { case (k, v) => v.length }
      val poisKeyPair = pois.keyBy(f => f.id)
      val clustersPois = Clusters(assignments.map(f => Cluster(f._1, f._2.map(x => poisKeyPair.lookup(x.toInt).head))))
      implicit val formats = DefaultFormats
      Serialization.writePretty(clustersPois, fileWriter)
    }
    
    def generatePois(sparkSession: SparkSession, poiCoordinates: RDD[(Long, Coordinate)], categoryIdValues: RDD[(Long, Categories)], poiCategoryIds: RDD[(Long, Set[Long])]): RDD[Poi] = {
      //poiCategoryIds.map(f => Poi(f._1, poiCoordinates.lookup(f._1).head, categories.lookup(f._1).head))
      val categoriesMap = categoryIdValues.collectAsMap()
      val poiCategoryIdsMap = poiCategoryIds.collectAsMap()
      poiCoordinates.map(f => Poi(f._1, f._2, {val categories = Categories(scala.collection.mutable.Set[String]())
                                              poiCategoryIdsMap(f._1).foreach(x => categories.categories ++=
                                                                                                     {if (categoriesMap.contains(x))  // some of the category id does not have corresponding category value
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
      System.setProperty("hadoop.home.dir", "C:\\winutil\\")
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
                                                ).sample(false, 0.002, 0).persist()
      val keys = poiVinna.keys.collect()
      println(s"Number of sampled poi in Vinna: ${keys.length}")

      
      // find all the categories of pois in Vinna
      val poiFlatCategories = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(categoryPOI))
      val poiCategoriesVinna = poiFlatCategories.filter(x => keys.contains(x.getSubject.toString().replace(poiPrefix, "").toLong))

      // from 'Node' to (poi_id, category_id) pairs, possible with duplicated keys
      val poiRawCategoriesVinna = poiCategoriesVinna.map(x => (x.getSubject.toString().replace(poiPrefix, "").toLong,
                                                         x.getObject.toString().replace(termPrefix, "").toLong))
      
      // get the categories for each poi
      val poiCategorySetVinna = poiRawCategoriesVinna.groupByKey().map(f => (f._1, f._2.toSet)).persist()
      println(s"Number of sampled poi in Vinna, with categories: ${poiCategorySetVinna.count()}")
      
      //oneHotEncoding(poiCategories, spark)
      
      // considering PIC https://spark.apache.org/docs/1.5.1/mllib-clustering.html, build ((sid, ()), (did, ())) RDD
      val pairwisePOICategorySet = poiCategorySetVinna.cartesian(poiCategorySetVinna).filter{ case (a, b) => a._1 < b._1 }
      
      // from ((sid, ()), (did, ())) to (sid, did, similarity)
      val pairwisePOISimilarity = pairwisePOICategorySet.map(x => (x._1._1.toLong, x._2._1.toLong,
                                                                  new Distances().jaccardSimilarity(x._1._2, x._2._2))).persist()
      
      // get the coordinates 
      
      // distance RDD, from (sid, did, similarity) to (sid, did, distance)
      //val distancePairs = pairwisePOISimilarity.map(x => (x._1, x._2, 1.0 - x._3))
      
      // generate coordindates in 2 dimension
      //val coordinates = multiDimensionScaling(distancePairs, numberPOIs, 2).map(x => (x(0), x(1)))
     
      // kmeans clustering, number of clusters 2
      //println(kmeansClustering(coordinates, spark, 2))
      
      // dbscan clustering, TODO solve scala version flicts with SANSA
      // dbscanClustering(coordinates, spark)
      
      // pic clustering, 3 centroids and 1 iterations
      val clusters = new PIC().picSparkML(pairwisePOISimilarity, 3, 1, spark)

      // aggregate category values based on category id
      val categoryIdValues = getCategoryValues(spark, dataRDD).persist()
      println(s"Number of categories: ${categoryIdValues.count()}")

      val poiVinnaCategoryIds = poiCategorySetVinna.flatMap(f => f._2).collect().toSet
      println(s"Number of categories in Vinna: ${poiVinnaCategoryIds.size}")

      val categoryVinnaIdValues = categoryIdValues.filter(f => poiVinnaCategoryIds.contains(f._1)).persist()
      println(s"Number of categories with value in Vinna: ${categoryVinnaIdValues.count()}")

      val pois = generatePois(spark, poiVinna, categoryVinnaIdValues, poiCategorySetVinna)
      println(s"number of poi: ${pois.count()}")
      writeClusteringResult(clusters, pois)
      
      // stop spark session
      fileWriter.close()
      spark.stop()
    }
}