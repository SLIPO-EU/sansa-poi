package eu.slipo.poi

import java.io.PrintWriter
import eu.slipo.datatypes.appConfig
import eu.slipo.utils.tomTomDataFiltering
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse
import net.sansa_stack.rdf.spark.io.{ ErrorParseMode, NTripleReader }
import java.io.{ File, FilenameFilter }
import com.typesafe.config.ConfigFactory
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import org.json4s.native.JsonMethods._
import java.io._
import org.json4s.DefaultFormats
import eu.slipo.datatypes.{ Cluster, Clusters }
import org.apache.spark.sql.DataFrame
import eu.slipo.datatypes.Poi
import org.apache.spark.sql.functions._
import eu.slipo.datatypes.Coordinate

object stayPointAlgo {
  def main(args: Array[String]): Unit = {
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

    //Reading N-Triples through SANSA NTriplereader class
    val dataRDD: RDD[Triple] = loadNTriple(conf.getString("slipo.data.input"), spark)
    println("Total number of triples: " + dataRDD.count().toString)

    val mapID = dataRDD.map(f => (f.getMatchSubject.toString(), f.getObject.toString()))
    val groupID = mapID.groupByKey().map(f => (f._1, f._2.toList))
    //clean or remove the unnecessary string from URI
    val cleanKV = removeURI(groupID)
    //Stay point algo
    val minTime = stayPointAlgo(cleanKV, spark, conf)
    // read the json file from Geo -semantic clustering algorithm
    readJson(conf.getString("slipo.data.json"), minTime, conf)
  }

  def loadNTriple(tripleFilePath: String, spark: SparkSession): RDD[Triple] = {
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

  def removeURI(groupID: RDD[(String, List[String])]): RDD[(String, (String, String, String, String))] = {
    val KVpair = groupID.map({ f =>
      val keyIdx = f._1.indexOf("id")
      val key = f._1.substring(keyIdx, keyIdx + 4)
      val y = f._2 match {
        case List(a, b, c, d) => (a, b, c, d)
      }
      val lat = y._1.substring(1, y._1.indexOf("^") - 1)
      val long = y._2.substring(1, y._2.indexOf("^") - 1)
      val starttime = y._3.substring(1, y._3.indexOf("^") - 1)
      val endtime = y._4.substring(1, y._4.indexOf("^") - 1)

      (key, (lat, long, starttime, endtime))
    })
    KVpair.foreach(println)
    KVpair
  }

  def stayPointAlgo(data: RDD[(String, (String, String, String, String))], spark: SparkSession, conf: Config): DataFrame = {

    import spark.implicits._
    // creating dataframe from rdd
    val rddToDF = data
      .map(w => xyz(w._1, w._2._1.toDouble, w._2._2.toDouble, w._2._3.replace("T", " "),
        w._2._4.replace("T", " "))).toDS()
    //calculating time difference where user stay and compare it with Tmin
    val timeDiff = rddToDF.withColumn("difference(min)", (unix_timestamp($"endtime") - unix_timestamp($"starttime")) / 60)
    val filterTmin = timeDiff.filter($"difference(min)" > conf.getString("slipo.min.time"))

    //calculating distance between two consecutive locations
    val crossJoinDf = filterTmin.as("d1").crossJoin(filterTmin.as("d2")).filter($"d1.id" === $"d2.id" && $"d1.lat" =!= $"d2.lat"
      && $"d1.endtime" < $"d2.starttime")
    val distancecal = crossJoinDf.withColumn("value", haversine(crossJoinDf("d1.lat"), crossJoinDf("d1.longitude"), crossJoinDf("d2.lat"), crossJoinDf("d2.longitude"))) //.show(false)
    val filterDist = distancecal.filter($"value" <= conf.getString("slipo.max.dist"))
    //filtering unnecessary columns from dataframe
    val selectLatLongCol = filterDist.select("d1.lat", "d1.longitude", "d2.lat", "d2.longitude")
    val countStayPointVisit = selectLatLongCol.groupBy("d1.lat", "d1.longitude", "d2.lat", "d2.longitude").count.sort($"count".desc).filter($"count" > 1)
    //calculating centroid of the location
    val calcentroid = countStayPointVisit.withColumn("predcited lat/long", calCentroid(countStayPointVisit("d1.lat"), countStayPointVisit("d1.longitude"), countStayPointVisit("d2.lat"), countStayPointVisit("d2.longitude")))
    calcentroid.show(false)
    calcentroid
  }

  def haversine = udf((lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
    val R = 6372.8 //radius in km
    import math._
    val dLat = (lat2 - lat1).toRadians
    val dLon = (lon2 - lon1).toRadians

    val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * asin(sqrt(a))
    R * c * 1000
  })

  def calCentroid = udf((lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
    val R = 6372.8 //radius in km
    import math._
    val dLat = (lat2 + lat1) / 2
    val dLon = (lon2 + lon1) / 2

    (dLat, dLon).toString()
  })

  case class xyz(id: String, lat: Double, longitude: Double, starttime: String, endtime: String)

  def readJson(jsonFilePath: String, minTime: DataFrame, conf: Config): Unit = {
    val stream = new FileInputStream(jsonFilePath)
    val stringFromJson = stringBuilder(stream)
    val json = try { parse(stringFromJson) } finally { stream.close() }
    implicit val formats = DefaultFormats
    val clustersJson = json.extract[Clusters]
    clusterSearch(clustersJson.clusters.head, minTime, conf)
  }

  def stringBuilder(fis: FileInputStream): String = {
    val br = new BufferedReader(new InputStreamReader(fis, "UTF-8"))
    val sb: StringBuilder = new StringBuilder
    var line: String = null
    while ({
      line = br.readLine()
      //println(line)
      line != null
    }) {
      sb.append(line)
      sb.append('\n')
    }
    sb.toString()
  }

  def clusterSearch(cluster: Cluster, minTime: DataFrame, config: Config) {
    val userWant = "Indian Resturant"
    val filterLatLong = minTime.select("predcited lat/long")
    val toRDD = filterLatLong.rdd.map(row => {
      val latLong = row.getString(0)
      latLong
    }).collect().mkString(",")

    val lat = toRDD.substring(1, toRDD.indexOf(",")).toDouble
    val latDec = BigDecimal(lat).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    val long = toRDD.substring(toRDD.indexOf(",") + 1).replace(")", "").toDouble
    val longDec = BigDecimal(long).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    var poiCluster: Array[Poi] = null
    var m = List[Coordinate]()
    println("lat=" + latDec + " " + "long=" + longDec)

    cluster.poi_in_cluster.foreach(poi => {
      if (latDec - 0.05 <= poi.coordinate.latitude && poi.coordinate.latitude < latDec + 0.05 && poi.coordinate.longitude < longDec + 0.05 && poi.coordinate.longitude > longDec - 0.05) {
        poiCluster = cluster.poi_in_cluster
      }
    })
    
    val searchStringCategories = poiCluster.filter(f => searchString(userWant, f.categories.categories.toString()))
    if (searchStringCategories.isEmpty)
      println(s"Stay point close to $latDec , $longDec is a good place to open an $userWant")
    else {
      val checkRating = searchStringCategories.map({ f =>
        if (f.review > config.getString("slipo.min.rating").toDouble) {
          println(s"Stay point is not a good place to open the $userWant")
        } else {
          println(s"Stay point close to $latDec , $longDec is a good place to open an $userWant")
        }
      })
    }
  }

  def searchString(x: String, y: String): Boolean = {
    val regexX = x.r
    val findXinY = regexX.findAllIn(y)
    if (findXinY.length > 0) {
      true
    } else {
      false
    }
  }
}
