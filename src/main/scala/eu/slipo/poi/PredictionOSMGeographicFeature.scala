package eu.slipo.poi

import org.apache.spark.sql._
import eu.slipo.algorithms.{Distances, Encoder, Kmeans, PIC}
import eu.slipo.datatypes._
import eu.slipo.utils.OSMdataProcessing
import eu.slipo.utils.Common
import org.json4s._
import com.typesafe.config.ConfigFactory
import org.json4s.jackson.Serialization
import java.io.PrintWriter

object PredictionOSMGeographicFeature {
  def main(args: Array[String]) {
    implicit val formats = DefaultFormats
    val conf = ConfigFactory.load()
    val spark = SparkSession.builder
      .master(conf.getString("slipo.spark.master"))
      .config("spark.serializer", conf.getString("slipo.spark.serializer"))
      .config("spark.executor.memory", conf.getString("slipo.spark.executor.memory"))
      .config("spark.driver.memory", conf.getString("slipo.spark.driver.memory"))
      .config("spark.driver.maxResultSize", conf.getString("slipo.spark.driver.maxResultSize"))
      .config("spark.ui.port", conf.getInt("slipo.spark.ui.port"))
      .config("spark.executor.cores", conf.getInt("slipo.spark.executor.cores"))
      .config("spark.executor.heartbeatInterval", conf.getLong("slipo.spark.executor.heartbeatInterval"))
      .config("spark.network.timeout", conf.getLong("slipo.spark.network.timeout"))  
      .appName(conf.getString("slipo.spark.app.name"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    println(spark.conf.getAll.mkString("\n"))
    val t0 = System.nanoTime()
    val tomTomData = new OSMdataProcessing(spark = spark, conf = conf)

    val pois = tomTomData.pois
    println("Number of pois: " + pois.count().toString)
  
    val poiCategorySetVienna = pois.map(poi => (poi.coordinate,poi.categories)).persist()
    println(pois.count())
    
    //location l to be examine for opening a shop
    val locationTargetd=Coordinate(16.3738346,48.20817324)
    
    val findDistance=poiCategorySetVienna.map(f=>distFrom(locationTargetd,f))
    val filterDistance=findDistance.filter(f=>f._2<200)
    val numberOfneighbors=filterDistance.count
    println("numberOfneighbors="+numberOfneighbors)
    
    //entropy
    val entropy=filterDistance.map(f=>f._1).distinct()
    val countDiffernetVenues=entropy.count()
    println("entropy="+countDiffernetVenues)
    
    //comeptitiveness
    val shopName="bar"
    val countNumberofBar=filterDistance.filter(f=>f._1.contains(shopName)).count()
    println("percenatge of bar to total number of nearby places.  ="+(countNumberofBar*100/numberOfneighbors)+"%")
    
    spark.stop()
  }

def distFrom(locationTargetd:Coordinate,otherCordinate:(Coordinate, String)):(String,Double,Coordinate)={
    val earthRadius = 6371000; //meters
    val dLat = Math.toRadians(locationTargetd.latitude-otherCordinate._1.latitude);
    val dLng = Math.toRadians(locationTargetd.longitude-otherCordinate._1.longitude);
    val a = Math.sin(dLat/2) * Math.sin(dLat/2) +
               Math.cos(Math.toRadians(otherCordinate._1.latitude)) * Math.cos(Math.toRadians(locationTargetd.latitude)) *
               Math.sin(dLng/2) * Math.sin(dLng/2);
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    val dist = (earthRadius * c);

    (otherCordinate._2,dist,otherCordinate._1)
    }

}