package eu.slipo.utils

import java.io.{ File, FilenameFilter }

import com.typesafe.config.Config
import org.apache.jena.graph.Triple
import eu.slipo.datatypes.{ Categories, Coordinate, Poi, PoiOSM }
import net.sansa_stack.rdf.spark.io.{ ErrorParseMode, NTripleReader }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * load TomTom dataset
 * @param spark SparkSession
 * @param conf Configuration
 */
class OSMdataProcessing(val spark: SparkSession, val conf: Config) extends Serializable {
  val dataRDD: RDD[Triple] = NTripleReader.load(spark, conf.getString("slipo.osm.input")).persist()
  var poiCoordinates: RDD[(Long, Coordinate)] = this.getPOICoordinates.persist()
  var categoriesOSM = this.getCat
  var join = poiCoordinates.join(categoriesOSM) //.join(cuisne)
  val pois = join.map(f => PoiOSM(f._1, f._2._1, f._2._2.toString))

  def getCat(): RDD[(Long, String)] = {
    val keyName = dataRDD.filter(f => f.getPredicate.toString().contains("https://wiki.openstreetmap.org/Key:amenity"))
    val filterSub = keyName.filter(f => f.getSubject.toString().contains("http://openstreetmap.org/node"))
    val getKV = filterSub.map(x => ((x.getSubject.toString().replace("http://openstreetmap.org/node/", "").toLong),
      x.getObject.toString()))
    getKV

  }

  def loadNTriple(tripleFilePath: String): RDD[Triple] = {
    val tripleFile = new File(tripleFilePath)
    if (tripleFile.isDirectory) {
      val files = tripleFile.listFiles(new FilenameFilter() {
        def accept(tripleFile: File, name: String): Boolean = {
          !(name.toString.contains("SUCCESS") || name.toLowerCase.endsWith(".crc"))
        }
      })
      var i = 0
      var triple_0 = NTripleReader.load(spark, files(0).getAbsolutePath, stopOnBadTerm = ErrorParseMode.SKIP)
      for (file <- files) {
        if (i != 0) {
          triple_0 = triple_0.union(NTripleReader.load(spark, file.getAbsolutePath))
        }
        i += 1
      }
      triple_0
    } else {
      NTripleReader.load(spark, tripleFile.getAbsolutePath, stopOnBadTerm = ErrorParseMode.SKIP)
    }
  }

  /**
   * get coordinate for all poi
   */
  def getPOICoordinates: RDD[(Long, Coordinate)] = {
    // get the coordinates of pois
    val pattern = "POINT (.+ .+)".r
    val poiCoordinatesString = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase("http://www.opengis.net/ont/geosparql#asWKT"))
    val removeOther = poiCoordinatesString.filter(f => (!f.getSubject.toString().contains("http://openstreetmap.org/way/")))
    val removeRelation = removeOther.filter(f => (!f.getSubject.toString().contains("http://openstreetmap.org/relation/")))
      .map(x => (
        x.getSubject.toString().replace("http://openstreetmap.org/node/", "").replace("/geom", "").toLong,
        pattern.findFirstIn(x.getObject.toString()).head.replace("POINT ", "")
        .replace("^^http://www.opengis.net/ont/geosparql#wktLiteral", "").replaceAll("^\"|\"$", "")))
    // transform to Coordinate object
    //removeRelation.coalesce(1)saveAsTextFile("/home/rajjat/Desktop/OSM/res2")
    val m = removeRelation.mapValues(x => {
      val coordinates = x.replace("(", "").replace(")", "").split(" ")
      Coordinate(coordinates(0).toDouble, coordinates(1).toDouble)
    })
    m

  }

}
