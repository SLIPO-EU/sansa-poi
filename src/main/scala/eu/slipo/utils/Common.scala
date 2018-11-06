package eu.slipo.utils

import java.io.PrintWriter

import java.awt.Color
import eu.slipo.datatypes.{Cluster, Clusters, Poi}
import eu.slipo.evaluation.{FScore, NMI, Purity, RI}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.mutable.ListBuffer
import breeze.plot._



object Common {

  /**
    * create a pair RDD and join with another pair RDD
    *
    * @param sparkContext
    * @param ids an array with poi id
    * @param pairs
    * @return an array of poi
    */
  def join(sparkContext: SparkContext, ids: Array[Long], pairs: RDD[(Long, Poi)]): Array[Poi] = {
    val idsPair = sparkContext.parallelize(ids).map(x => (x, x))
    idsPair.join(pairs).map(x => x._2._2).collect()
  }

  /**
    * serialize clustering results to file
    *
    * @param sparkContext
    * @param clusters clustering results
    * @param pois pois object
    * @return
    */
  def writeClusteringResult(sparkContext: SparkContext, clusters: Map[Int, Array[Long]], pois: RDD[Poi], fileWriter: PrintWriter): Clusters = {
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val poisKeyPair = pois.keyBy(f => f.poi_id).persist()
    val clustersPois = Clusters(assignments.size, assignments.map(_._2.length).toArray, assignments.map(f => Cluster(f._1, join(sparkContext, f._2, poisKeyPair))))
    implicit val formats = DefaultFormats
    Serialization.writePretty(clustersPois, fileWriter)
    clustersPois
  }

  /**
    * plot clustering results
    * @param listClusters a list of clustering results
    * @param pathSave path to save plot
    */
  def plotClusterResults(listClusters: List[Clusters], pathSave: String): Unit ={
    var statics = new ListBuffer[Map[String, Double]]()
    val f = Figure()
    val plt: Plot = f.subplot(0)
    val colors = List(Color.YELLOW, Color.BLUE, Color.RED, Color.BLACK) //, Color.GREEN
    val labels = List("OntHot KM", "Word2Vec KM", "PIC", "MDS KM") //, "MDS KM"
    var counter = 0
    val indexes: List[Double] = List(0.0, 1.0, 2.0, 3.0)
    listClusters.foreach(clusters => {
      val metrics: ListBuffer[Double] = new ListBuffer[Double]
      metrics.append(new Purity(clusters).calPurity())
      metrics.append(new NMI(clusters).calNMI())
      val (ri, tp, fp, tn, fn) = new RI(clusters).calRandInformationFScore()
      metrics.append(ri)
      metrics.append(new FScore().calFScore(fp, tp, fn))
      println(metrics.mkString(";"))
      println(indexes.mkString(","))
      plt += scatter(indexes, metrics, {_ => 0.025}, { _:Int => colors(counter)},  {_ => labels(counter)})
      counter += 1
    })
    plt.title = "Clustering Evaluation"
    plt.xlabel = "Evaluation Metric"
    plt.ylabel = "Metric Value"
    plt.ylim(0.0, 1.0)
    plt.setYAxisDecimalTickUnits()
    plt.xlim(-1.0, 5.0)
    //plt.legend
    f.saveas("results/clusteringEvaMetrics.png")
  }
}
