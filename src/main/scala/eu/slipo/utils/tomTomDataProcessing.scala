package eu.slipo.utils

import org.apache.jena.graph.Triple
import eu.slipo.datatypes.{Categories, Coordinate, Poi, appConfig}
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * load TomTom dataset
  * @param spark SparkSession
  * @param conf Configuration
  */
class tomTomDataProcessing(val spark: SparkSession, val conf: appConfig) extends Serializable {

  val dataRDD: RDD[Triple] = NTripleReader.load(spark, conf.dataset.input).persist()
  var poiCoordinates: RDD[(Long, Coordinate)] = this.getPOICoordinates(16.192851, 16.593533, 48.104194, 48.316388).sample(withReplacement = false, fraction = 0.001, seed = 0).persist()
  var poiFlatCategoryId: RDD[(Long, Long)] = this.getPOIFlatCategoryId
  var poiCategoryId: RDD[(Long, Set[Long])] = this.getCategoryId(poiCoordinates, poiFlatCategoryId)
  var poiCategoryValueSet: RDD[(Long, Categories)] = this.getCategoryValues
  var poiCategories: RDD[(Long, Categories)] = this.getPOICategories(poiCoordinates, poiFlatCategoryId, poiCategoryValueSet)
  var pois: RDD[Poi] = poiCoordinates.join(poiCategories).map(x => Poi(x._1, x._2._1, x._2._2))

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
      && (x._2.latitude >= la_min && x._2.latitude <= la_max)).persist()
  }

  /**
    * get coordinate for all poi
    */
  def getPOICoordinates: RDD[(Long, Coordinate)] ={
    // get the coordinates of pois
    val pattern = "POINT(.+ .+)".r
    val poiCoordinatesString = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(conf.dataset.coordinatesPredicate))
      .map(x => (x.getSubject.toString().replace(conf.dataset.poiPrefix, "").replace("/geometry", "").toLong,
        pattern.findFirstIn(x.getObject.toString()).head.replace("POINT", "")
          .replace("^^http://www.opengis.net/ont/geosparql#wktLiteral", "").replaceAll("^\"|\"$", "")))
    // transform to Coordinate object
    poiCoordinatesString.mapValues(x => {
        val coordinates = x.replace("(", "").replace(")", "").split(" ")
        Coordinate(coordinates(0).toDouble, coordinates(1).toDouble)
      }).persist()
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
    val poiFlatCategories = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(conf.dataset.categoryPOI))
    poiFlatCategories.map(x => (
      x.getSubject.toString().replace(conf.dataset.poiPrefix, "").toLong,
      x.getObject.toString().replace(conf.dataset.termPrefix, "").toLong)
    ).persist()
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
    val poiCategorySets = poiFlatCategoryId.map(f => (f._2, f._1)).join(poiCategoryValueSet).map(f => (f._2._1, f._2._2)).groupByKey().persist()
    // from (poi_unique, Iterable(Categories)) join-> (poi_unique, (Coordinate, Iterable(Categories))) map-> (poi_unique, Categories)
    poiCoordinates.join(poiCategorySets).map(x => (x._1, Categories(collection.mutable.Set(x._2._2.flatMap(_.categories).toList:_*)))).persist()
  }

  /**
    * get (category_id, Categories)
    * @return RDD with category values for category id
    */
  def getCategoryValues: RDD[(Long, Categories)] = {
    // get category id(s)
    val categoryTriples = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(conf.dataset.termValueUri))
    // get category id and it's corresponding values
    val categoriesIdValues = categoryTriples.map(x => (
      x.getSubject.toString().replace(conf.dataset.termPrefix, "").toLong,
      x.getObject.toString().replaceAll("\"", "")))
    // group by id and put all values of category to a set
    categoriesIdValues.groupByKey().map(x => (x._1, Categories(scala.collection.mutable.Set(x._2.toList: _*)))).persist()
  }

  /**
    * get (poi_unique, poi_category_id_set)
    * @param poiCoordinates (poi_unique, Coordinate)
    * @param poiFlatCategoryId (poi, category_id)
    */
  def getCategoryId(poiCoordinates: RDD[(Long, Coordinate)], poiFlatCategoryId: RDD[(Long, Long)]): RDD[(Long, Set[Long])] = {
    poiCoordinates.join(poiFlatCategoryId.groupByKey()).map(x => (x._1, x._2._2.toSet))
  }
}
