package eu.slipo.poi

import java.net.URI
import java.io.PrintWriter

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleRDD
import net.sansa_stack.rdf.spark.analytics
import org.apache.spark.rdd._
import org.apache.jena.graph.Triple
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.PowerIterationClustering


object poiClustering {
  
    /*
     * Jaccard Similarity Coefficient between two sets of categories corresponding to two pois
     * */
    def jaccardSimilarity(x: Iterable[String], y: Iterable[String]): Double = {
      val x_ = x.toSet
      val y_ = y.toSet
      val union_l = x_.toList.length + y_.toList.length
      val intersect_l = x_.intersect(y_).toList.length
      intersect_l / (union_l - intersect_l)
    }
    
    def main(args: Array[String]){
      
      val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader resources/data/tomtom_pois_austria_v0.3.nt")
      .getOrCreate()
      
      // constants
      val dataSource = "resources/data/tomtom_pois_austria_v0.3.nt"  // there are 312385 pois
      val typePOI = "http://slipo.eu/def#POI"
      val categoryPOI = "http://slipo.eu/def#category"
      val termPOI = "http://slipo.eu/def#termValue"
      val poiPrefix = "http://slipo.eu/id/poi/"
      val termPrefix = "http://slipo.eu/id/term/"
      val fileWriter = new PrintWriter("resources/results/clustering_result.txt")
      
      // read NTriple file, get RDD contains triples
      val data = NTripleReader.load(sparkSession, dataSource)
      
      // find all the categories of pois
      val poiFlatCategories = data.filter(x => x.getPredicate.toString().equalsIgnoreCase(categoryPOI))
      
      // from 'Node' to string, and remove common prefix
      val poiRawCategories = poiFlatCategories.map(x => (x.getSubject.toString().replace(poiPrefix, ""), x.getObject.toString().replace(termPrefix, "")))
      
      // get the categories for each poi TODO not encourage to use groupByKey as it is slow for large dataset
      // get 1% of the complete dataset to reduce the computation costs
      val poiCategories = poiRawCategories.groupByKey().sample(false, 0.01, 0)
      val numPOIs = poiCategories.count().toString()
      fileWriter.println(s"Number of POIs: $numPOIs")
      
      // considering PIC https://spark.apache.org/docs/1.5.1/mllib-clustering.html, build ((sid, ()), (did, ())) RDD
      val pairwisePOICategories = poiCategories.cartesian(poiCategories).filter{ case (a, b) => a._1.toInt < b._1.toInt }
      
      // from ((sid, ()), (did, ())) to (sid, did, similarity)
      val pairwisePOISimilarity = pairwisePOICategories.map(x => (x._1._1.toString().toLong, x._2._1.toString().toLong, jaccardSimilarity(x._1._2, x._2._2)))
      
      // run pic clustering, 500 centroids and 5 iterations
      val model = new PowerIterationClustering().setK(500).setMaxIterations(5).setInitializationMode("degree").run(pairwisePOISimilarity)
      val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
      val assignments = clusters.toList.sortBy { case (k, v) => v.length }
      val assignmentsStr = assignments.map { case (k, v) => s"$k -> ${v.sorted.mkString("[", ",", "]")}"}.mkString("\n")
      val sizesStr = assignments.map {_._2.length}.sorted.mkString("(", ",", ")")
      
      fileWriter.println(s"Cluster assignments:\n $assignmentsStr\n")
      fileWriter.println(s"cluster sizes:\n $sizesStr")
      
      // stop spark session
      sparkSession.stop()
    }
}