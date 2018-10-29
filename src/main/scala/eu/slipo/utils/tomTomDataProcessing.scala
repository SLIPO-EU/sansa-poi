package eu.slipo.utils

import java.io.{File, FilenameFilter}

import com.typesafe.config.Config
import org.apache.jena.graph.Triple
import eu.slipo.datatypes.{Categories, Coordinate, Poi}
import net.sansa_stack.rdf.spark.io.{ErrorParseMode, NTripleReader}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.junit.Ignore


/**
  * load TomTom dataset
  * @param spark SparkSession
  * @param conf Configuration
  */
class tomTomDataProcessing(val spark: SparkSession, val conf: Config) extends Serializable {

  //val dataRDD: RDD[Triple] = NTripleReader.load(spark, conf.getString("slipo.data.input")).persist()
  val dataRDD: RDD[Triple] = loadNTriple(conf.getString("slipo.data.input"))
  println("Total number of pois: " + dataRDD.count().toString)
  //var poiCoordinates: RDD[(Long, Coordinate)] = this.getPOICoordinates(16.192851, 16.593533, 48.104194, 48.316388).sample(withReplacement = false, fraction = 0.005, seed = 0)
  var poiCoordinates: RDD[(Long, Coordinate)] = this.getPOICoordinates.persist()
  var poiFlatCategoryId: RDD[(Long, Long)] = this.getPOIFlatCategoryId
  var poiCategoryId: RDD[(Long, Set[Long])] = this.getCategoryId(poiCoordinates, poiFlatCategoryId).persist()
  var poiCategoryValueSet: RDD[(Long, Categories)] = this.getCategoryValues  //(category_id, Categories)
  var poiCategories: RDD[(Long, Categories)] = this.getPOICategories(poiCoordinates, poiFlatCategoryId, poiCategoryValueSet).persist()  // (poi_id, Categories)
  val poiYelpCategories: RDD[(Long, (Categories, Double))] = this.getYelpCategories(dataRDD).persist()
  println("Yelp category: " + poiYelpCategories.count().toString)
  var pois: RDD[Poi] = {if(!poiYelpCategories.isEmpty()){
    println("Get POIs from Yelp")
    //val poiAllCategories: RDD[(Long, Categories, Double)] = poiCategories.join(poiYelpCategories).map(x => (x._1, (Categories(x._2._1.categories++x._2._2._1.categories), x._2._2._2))
    val poiAllCategories: RDD[(Long, (Categories, Double))] = poiYelpCategories.join(poiCategories).map(x => (x._1, (Categories(x._2._1._1.categories++x._2._2.categories), x._2._1._2)))
    filterJoin(poiAllCategories.collectAsMap(), poiCoordinates)
    //poiCoordinates.join(spark.sparkContext.broadcast(poiAllCategoriesCollection)).map(x => Poi(x._1, x._2._1, x._2._2._1, x._2._2._2)).persist()
  }else{
    println("Get POIs from TOM TOM")
    poiCoordinates.join(poiCategories).map(x => Poi(x._1, x._2._1, x._2._2, 0.0)).persist()
  }}

  def broadCastJoin(poiAllCategories: RDD[(Long, (Categories, Double))], poiCoordinates: RDD[(Long, Coordinate)]): RDD[Poi] = {
    val poiAllCategoriesMap: Broadcast[collection.Map[Long, (Categories, Double)]] = spark.sparkContext.broadcast(poiAllCategories.collectAsMap())
    poiCoordinates.flatMap{
      case(key, value) => poiAllCategoriesMap.value.get(key).map{
        x => Poi(key, value, x._1, x._2)
      }
    }
  }

  def filterJoin(poiAllCategories: scala.collection.Map[Long, (Categories, Double)], poiCoordinates: RDD[(Long, Coordinate)]): RDD[Poi] = {
    val poiCoordinatesFiltered = poiCoordinates.filter{
      x => {
        poiAllCategories.contains(x._1)
      }
    }
    poiCoordinatesFiltered.map{
      x => {
        val poiCategoryReview: (Categories, Double) = poiAllCategories.get(x._1).head
        Poi(x._1, x._2, poiCategoryReview._1, poiCategoryReview._2)
      }
    }
  }

  def loadNTriple(tripleFilePath: String): RDD[Triple] = {
    val tripleFile = new File(tripleFilePath)
    if(tripleFile.isDirectory){
      val files = tripleFile.listFiles(new FilenameFilter() {
        def accept(tripleFile: File, name: String):Boolean= {
          !(name.toString.contains("SUCCESS") || name.toLowerCase.endsWith(".crc"))
        }
      })
      var i = 0
      var triple_0 = NTripleReader.load(spark, files(0).getAbsolutePath, stopOnBadTerm = ErrorParseMode.SKIP)
      for(file <- files){
        if(i!=0){
          triple_0 = triple_0.union(NTripleReader.load(spark, file.getAbsolutePath))
        }
        i+=1
      }
      triple_0
    }
    else{
      NTripleReader.load(spark, tripleFile.getAbsolutePath, stopOnBadTerm = ErrorParseMode.SKIP)
    }
  }


  /**
    * @param poiCoordinates super set of poi with coordinates
    * @param lo_min min longitude
    * @param lo_max max longitude
    * @param la_min min latitude
    * @param la_max max latitude
    * @return pois within certain coordinates
    */
  def filterCoordinates(poiCoordinates: RDD[(Long, Coordinate)], lo_min: Double, lo_max: Double, la_min: Double, la_max: Double): RDD[(Long, Coordinate)] = {
    poiCoordinates.filter(x => (x._2.longitude >= lo_min && x._2.longitude <= lo_max)
      && (x._2.latitude >= la_min && x._2.latitude <= la_max))
  }

  /**
    * get coordinate for all poi
    */
  def getPOICoordinates: RDD[(Long, Coordinate)] ={
    // get the coordinates of pois
    val pattern = "POINT(.+ .+)".r
    val poiCoordinatesString = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(conf.getString("slipo.data.coordinatesPredicate")))
      .map(x => (x.getSubject.toString().replace(conf.getString("slipo.data.poiPrefix"), "").replace("/geometry", "").toLong,
        pattern.findFirstIn(x.getObject.toString()).head.replace("POINT", "")
          .replace("^^http://www.opengis.net/ont/geosparql#wktLiteral", "").replaceAll("^\"|\"$", "")))
    // transform to Coordinate object
    poiCoordinatesString.mapValues(x => {
        val coordinates = x.replace("(", "").replace(")", "").split(" ")
        Coordinate(coordinates(0).toDouble, coordinates(1).toDouble)
      })
  }

  /**
    * load data filter on geo-coordinates
    * @param lo_min min longitude
    * @param lo_max max longitude
    * @param la_min min latitude
    * @param la_max max latitude
    */
  def getPOICoordinates(lo_min: Double, lo_max: Double, la_min: Double, la_max: Double): RDD[(Long, Coordinate)] ={
    this.filterCoordinates(poiCoordinates = this.getPOICoordinates, lo_min=lo_min, lo_max=lo_max, la_min=la_min, la_max=la_max)
  }

  /**
    *
    * @return (poi, category_id)
    */
  def getPOIFlatCategoryId: RDD[(Long, Long)] ={
    val poiFlatCategories = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(conf.getString("slipo.data.categoryPOI")))
    poiFlatCategories.map(x => (
      x.getSubject.toString().replace(conf.getString("slipo.data.poiPrefix"), "").toLong,
      x.getObject.toString().replace(conf.getString("slipo.data.termPrefix"), "").toLong)
    )
  }

  /**
    * get (poi_unique, Categories)
    * @param poiCoordinates (poi_unique, Coordinate)
    * @param poiFlatCategoryId (poi, category_id)
    * @param poiCategoryValueSet (category_id, Categories)
    * @return (poi, Categories)
    */
  def getPOICategories(poiCoordinates: RDD[(Long, Coordinate)], poiFlatCategoryId: RDD[(Long, Long)], poiCategoryValueSet: RDD[(Long, Categories)]): RDD[(Long, Categories)] ={
    // from (poi, category_id) map-> (category_id, poi) join-> (category_id, (poi, Categories)) map-> (poi, Categories) groupByKey-> (poi_unique, Iterable(Categories))
    val poiCategorySets = poiFlatCategoryId.map(f => (f._2, f._1)).join(poiCategoryValueSet).map(f => (f._2._1, f._2._2)).groupByKey()
    // from (poi_unique, Iterable(Categories)) join-> (poi_unique, (Coordinate, Iterable(Categories))) map-> (poi_unique, Categories)
    poiCoordinates.join(poiCategorySets).map(x => (x._1, Categories(Set(x._2._2.flatMap(_.categories).toList:_*))))
  }

  /**
    * get (category_id, Categories)
    * @return RDD with category values for category id
    */
  def getCategoryValues: RDD[(Long, Categories)] = {
    // get category id(s)
    val categoryTriples = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(conf.getString("slipo.data.termValueUri")))
    // get category id and it's corresponding values
    val categoriesIdValues = categoryTriples.map(x => (
      x.getSubject.toString().replace(conf.getString("slipo.data.termPrefix"), "").toLong,
      x.getObject.toString().replaceAll("\"", "")))
    // group by id and put all values of category to a set
    categoriesIdValues.groupByKey().map(x => (x._1, Categories(Set(x._2.toList: _*))))
  }

  /**
    * get (poi_unique, poi_category_id_set)
    * @param poiCoordinates (poi_unique, Coordinate)
    * @param poiFlatCategoryId (poi, category_id)
    */
  def getCategoryId(poiCoordinates: RDD[(Long, Coordinate)], poiFlatCategoryId: RDD[(Long, Long)]): RDD[(Long, Set[Long])] = {
    poiCoordinates.join(poiFlatCategoryId.groupByKey()).map(x => (x._1, x._2._2.toSet))
  }


  def getYelpCategories(mergedRDD: RDD[Triple]): RDD[(Long, (Categories, Double))] = {
    val yelpPOICategory = mergedRDD.filter(triple => triple.getPredicate.toString.equalsIgnoreCase(conf.getString("yelp.data.categoryPOI")))
    println(conf.getString("yelp.data.rating"))
    val yelpPOIRating = mergedRDD.filter(triple => triple.getPredicate.toString.contains(conf.getString("yelp.data.rating")))
    println("category")
    println(yelpPOICategory.count())
    println("rating")
    println(yelpPOIRating.count())
    val yelpPOICategoryMapped = yelpPOICategory.map(triple => (
      triple.getSubject.toString().replace(conf.getString("slipo.data.poiPrefix"), "").toLong,
      triple.getObject.toString().replaceAll("\"", "")
    ))
    val yelpPOIRatingMapped = yelpPOIRating.map(triple => (
      triple.getSubject.toString().replace(conf.getString("slipo.data.poiPrefix"), "").toLong,
      triple.getObject.getLiteralValue.toString.toDouble
      ))
    yelpPOICategoryMapped.groupByKey().join(yelpPOIRatingMapped).map(x => (x._1, (Categories(Set(x._2._1.toList: _*)), x._2._2)))
  }
}
