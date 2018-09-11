package eu.slipo.utils

import java.io.PrintWriter
import java.util.regex.Pattern

import com.typesafe.config.{Config, ConfigFactory}
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang


object mergeTomTomYelp extends Serializable {

  implicit val formats = DefaultFormats
  val conf = ConfigFactory.load()
  val fileWriter = new PrintWriter(conf.getString("yelp.slipo.merged_file"))

  def mergeYelpSlipoData(slipoRDD: RDD[Triple], yelpRDD: RDD[Triple], conf: Config): Unit = {
    val yelpCategories = yelpRDD.filter(triple => triple.getPredicate.hasURI("http://slipo.eu/hasYelpCategory") || triple.getPredicate.hasURI("http://slipo.eu/hasRating"))
    val mergedRDD = yelpCategories.union(slipoRDD).persist()

    mergedRDD.foreach(t => {
      val objStr =
        if (t.getObject.isLiteral) {
          val splittedObject: Array[String] = t.getObject.toString.split("^^")
          val head = splittedObject.head
          val obj = if (head.contains("^^")){
            val parts = head.split("\\^\\^")
            parts.head
          }else{
            splittedObject.head
          }
          obj//+"^^<"+t.getObject.getLiteralDatatypeURI+">"
        } else {
          s"<${t.getObject}>"
        }

      fileWriter.println(s"<${t.getSubject}> <${t.getPredicate}> ${objStr.replace("\n", "")} .")
    })
    //mergedRDD.saveAsNTriplesFile("data/merged_tomtom_yelp")
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master(conf.getString("slipo.spark.master"))
      .config("spark.serializer", conf.getString("slipo.spark.serializer"))
      .config("spark.executor.memory", conf.getString("slipo.spark.executor.memory"))
      .config("spark.driver.memory", conf.getString("slipo.spark.driver.memory"))
      .config("spark.driver.maxResultSize", conf.getString("slipo.spark.driver.maxResultSize"))
      .appName(conf.getString("slipo.spark.app.name"))
      .getOrCreate()

    var lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)("input")
    val slipoDataRDD: RDD[Triple] = NTripleReader.load(spark, conf.getString("slipo.merge.input")).persist()
    val yelpDataRDD: RDD[Triple] = NTripleReader.load(spark, conf.getString("yelp.data.input")).persist()
    mergeYelpSlipoData(slipoDataRDD, yelpDataRDD, conf)
    fileWriter.close()
  }
}
