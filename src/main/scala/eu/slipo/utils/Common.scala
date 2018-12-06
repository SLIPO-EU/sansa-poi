package eu.slipo.utils

import java.io._
import java.awt.Color

import eu.slipo.datatypes.{Cluster, Clusters, Poi}
import eu.slipo.evaluation.{FScore, NMI, Purity, RI}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.apache.jena.graph.{Triple, NodeFactory}

import scala.collection.mutable.ListBuffer
import breeze.plot._



object Common {

  val prefixID = "http://clustering.slipo.eu/poi/"
  val prefixCategory = "http://clustering.slipo.eu/hasCategories"
  val prefixCoordinate = "http://clustering.slipo.eu/hasCoordinate/"

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
    * serialize clustering results to json file
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
    * serialize clustering results to .nt file
    */
  def seralizeToNT(sparkContext: SparkContext, clusters: Map[Int, Array[Long]], pois: RDD[Poi]): Unit ={
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val poisKeyPair = pois.keyBy(f => f.poi_id).persist()
    val newAssignment = assignments.map(f => (f._1, sparkContext.parallelize(f._2).map(x => (x, x)).join(poisKeyPair).map(x => ( x._2._2.poi_id, x._2._2.categories, x._2._2.coordinate)).collect()))
    val newAssignmentRDD = sparkContext.parallelize(newAssignment)
    println(newAssignmentRDD.count())
    val newAssignmentRDDTriple = newAssignmentRDD.map(cluster => (cluster._1, cluster._2.flatMap(poi =>
                                          {List(new Triple(NodeFactory.createURI(prefixID + poi._1.toString),
                                                    NodeFactory.createURI(prefixCategory),
                                                    NodeFactory.createLiteral(poi._2.categories.mkString(","))),
                                                new Triple(NodeFactory.createURI(prefixID + poi._1.toString),
                                                  NodeFactory.createURI(prefixCoordinate),
                                                  NodeFactory.createLiteral((poi._3.latitude, poi._3.longitude).toString()))
                                          )}
                                            ).toList)
    )
    newAssignmentRDDTriple.saveAsTextFile("results/triples")
    //println(newAssignmentRDDTriple.count())
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
      println(counter)
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

  /**
    * Write clustering results to file
    */
  def writeClusteringResults(listClusters: List[Clusters], pathSave: String): Unit ={
    val CSV_DILEMITER: String = ","
    val headers = List("Algorithms", "Purity", "NMI", "RI", "F Score")
    val algorithms = List("OntHot KM", "Word2Vec KM", "PIC", "MDS KM")
    try {
      val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pathSave), "UTF-8"))
      // write header to csv
      val headerLine = new StringBuffer()
      headers.foreach(header => {
        headerLine.append(header)
        headerLine.append(CSV_DILEMITER)
      })
      bw.write(headerLine.toString)
      bw.newLine()
      // write clustering result metrics
      var counter = 0
      listClusters.foreach(clusters => {
        val oneLine = new StringBuffer()
        oneLine.append(algorithms(counter))
        oneLine.append(CSV_DILEMITER)
        val metrics: ListBuffer[Double] = new ListBuffer[Double]
        metrics.append(new Purity(clusters).calPurity())
        metrics.append(new NMI(clusters).calNMI())
        val (ri, tp, fp, tn, fn) = new RI(clusters).calRandInformationFScore()
        metrics.append(ri)
        metrics.append(new FScore().calFScore(fp, tp, fn))
        metrics.foreach(metric => {
          oneLine.append(metric)
          oneLine.append(CSV_DILEMITER)
        })
        bw.write(oneLine.toString)
        bw.newLine()
        counter += 1
      })
      bw.flush()
      bw.close()
    } catch {
      case ioe: IOException =>
      case e: Exception =>
    }
  }
}
