package eu.slipo.poi

import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.jena.graph.Triple
import eu.slipo.algorithms.{Distances, Encoder, Kmeans, PIC}
import eu.slipo.datatypes.{Cluster, Clusters, Coordinate, Poi, Categories, appConfig, DistanceMatrix, Distance}
import org.json4s._
import org.json4s.DefaultFormats._
import org.json4s.native.JsonMethods.parse
import org.json4s.jackson.Serialization

object poiClustering {

  /**
   * get (category_id, category_values_set)
   *
   * @param sparkSession
   * @param data RDD containing triples
   * @param conf Application configuration
   * @return RDD with category values for category id
   */
  def getCategoryValues(sparkSession: SparkSession, data: RDD[Triple], conf: appConfig): RDD[(Long, Categories)] = {
    // get category id(s)
    val categoryIds = data.filter(x => x.getPredicate.toString().equalsIgnoreCase(conf.dataset.termValueUri))
    // get category id and it's corresponding values
    val categoriesIdValues = categoryIds.map(x => (
      x.getSubject.toString().replace(conf.dataset.termPrefix, "").toLong,
      x.getObject.toString().replaceAll("\"", "")))
    // group by id and put all values of category to a set
    categoriesIdValues.groupByKey().map(x => (x._1, Categories(scala.collection.mutable.Set(x._2.toList: _*))))
  }

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
  def writeClusteringResult(sparkContext: SparkContext, clusters: Map[Int, Array[Long]], pois: RDD[Poi], fileWriter: PrintWriter): Unit = {
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val poisKeyPair = pois.keyBy(f => f.poi_id).persist()
    val clustersPois = Clusters(assignments.size, assignments.map(_._2.length).toArray, assignments.map(f => Cluster(f._1, join(sparkContext, f._2, poisKeyPair))))
    implicit val formats = DefaultFormats
    Serialization.writePretty(clustersPois, fileWriter)
  }

  /**
   * Build a list of Poi objects
   *
   * @param sparkSession
   * @param poiCoordinates with pairs of poi id and it's coordinates
   * @param categoryIdValues category and it's corresponding values
   * @param poiCategoryIds poi and it's corresponding category ids
   * @return RDD with poi objects
   */
  def generatePois(sparkSession: SparkSession, poiCoordinates: RDD[(Long, Coordinate)],
    categoryIdValues: RDD[(Long, Categories)], poiCategoryIds: RDD[(Long, Set[Long])]): RDD[Poi] = {
    val categoriesMap = categoryIdValues.collectAsMap()
    val poiCategoryIdsMap = poiCategoryIds.collectAsMap()
    poiCoordinates.map(f => Poi(f._1, f._2,
      {
        val categories = Categories(scala.collection.mutable.Set[String]())
        poiCategoryIdsMap(f._1).foreach(x => categories.categories ++=
          { // some of the category id does not have corresponding category value
            if (categoriesMap.contains(x)) {
              categoriesMap(x).categories
            } else {
              scala.collection.mutable.Set[String](s"unknown for category id : $x")
            }
          })
        categories
      })).persist()
  }

  /**
   * @param poiID id of a poi
   * @return an array of subject in RDF triples with related to this poi
   */
  def createSubjects(poiID: Long): ArrayBuffer[String] = {
    val subjects = ArrayBuffer[String]()
    val id = "http://slipo.eu/id/poi/".concat(poiID.toString())
    subjects.+=(id)
    subjects.+=(id.concat("/address"))
    subjects.+=(id.concat("/phone"))
    subjects.+=(id.concat("/geometry"))
    subjects.+=(id.concat("/name"))
    subjects.+=(id.concat("/accuracy_info"))
    subjects.+=(id.concat("/brandname"))
    subjects
  }

  /**
   * @param viennaKeys id of pois in Vienna
   * @param dataRDD RDD containing triples
   * @param spark
   * @return
   */
  def getTriples(viennaKeys: Array[Long], dataRDD: RDD[Triple], spark: SparkSession, viennaTriplesWriter: PrintWriter) {
    val subjects = ArrayBuffer[String]()
    for (i <- 0 until viennaKeys.length - 1) {
      subjects ++= createSubjects(i)
    }
    val dataRDDPair = dataRDD.map(f => (f.getSubject.getURI, f)).persist()
    val subjectsRDD = spark.sparkContext.parallelize(subjects.toSet.toList).map(f => (f, f)).persist()
    val viennaTriples = subjectsRDD.join(dataRDDPair).map(f => f._2._2)
    viennaTriples.foreach(f => viennaTriplesWriter.println(f.getSubject.getURI + " " + f.getPredicate.getURI + " " + f.getObject.toString()))
    val viennaCatgoriesObjects = viennaTriples.filter(f => f.getPredicate.getURI.equals("http://slipo.eu/def#category")).map(f => f.getObject.getURI).distinct()
    val viennaPoiCategoriesRDD = viennaCatgoriesObjects.map(f => (f, f))
    val viennaCategoryTriples = viennaPoiCategoriesRDD.join(dataRDDPair).map(f => f._2._2)
    val temp = viennaCategoryTriples.map(f => (f.getSubject.getURI + f.getPredicate.getURI + f.getObject.toString(), f))
    temp.reduceByKey((v1, v2) => v1).foreach(f => viennaTriplesWriter.println(f._2.getSubject.getURI + " " + f._2.getPredicate.getURI + " " + f._2.getObject.toString()))
  }

  /**
   * @param poiCoordinates
   * @param lo_min
   * @param lo_max
   * @param la_min
   * @param la_max
   * @return pois within certain coordinates  
   */
  def filterCoordinates(poiCoordinates: RDD[(Long, Coordinate)], lo_min: Double, lo_max: Double, la_min: Double, la_max: Double): RDD[(Long, Coordinate)] = {
    poiCoordinates.filter(x => (x._2.longitude >= lo_min && x._2.longitude <= lo_max)
      && (x._2.latitude >= la_min && x._2.latitude <= la_max))
  }
  
  /**
   * main function
   */
  def main(args: Array[String]) {
    implicit val formats = DefaultFormats
    val raw_conf = scala.io.Source.fromFile("src/main/resources/conf.json").reader()
    val conf = parse(raw_conf).extract[appConfig]
    val profileWriter = new PrintWriter(conf.clustering.profile)
    val picFileWriter = new PrintWriter(conf.clustering.pic)
    val oneHotKMFileWriter = new PrintWriter(conf.clustering.oneHotKM)
    val mdsKMFileWriter = new PrintWriter(conf.clustering.mdsKM)
    val word2VecKMFileWriter = new PrintWriter(conf.clustering.word2VecKM)
    val picDistanceMatrixWriter = new PrintWriter(conf.clustering.picDistanceMatrix)
    //val viennaTriplesWriter = new PrintWriter("results/vienna.nt")


    // System.setProperty("hadoop.home.dir", "C:\\Hadoop") // for Windows system
    val spark = SparkSession.builder
      .master(conf.spark.master)
      .config("spark.serializer", conf.spark.spark_serializer)
      .config("spark.executor.memory", conf.spark.spark_executor_memory)
      .config("spark.driver.memory", conf.spark.spark_driver_memory)
      .config("spark.driver.maxResultSize", conf.spark.spark_driver_maxResultSize)
      .appName(conf.spark.app_name)
      .getOrCreate()

    val t0 = System.nanoTime()
    // read NTriple file, get RDD contains triples
    val dataRDD = NTripleReader.load(spark, conf.dataset.input).persist()
    // get the coordinates of pois
    val pattern = "POINT(.+ .+)".r
    val poiCoordinates = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(conf.dataset.coordinatesPredicate))
      .map(x => (x.getSubject.toString().replace(conf.dataset.poiPrefix, "").replace("/geometry", "").toLong,
        pattern.findFirstIn(x.getObject.toString()).head.replace("POINT", "")
        .replace("^^http://www.opengis.net/ont/geosparql#wktLiteral", "").replaceAll("^\"|\"$", "")))

    // transform to Coordinate object
    val poiCleanCoordinates = poiCoordinates.mapValues(x => {
      val coordinates = x.replace("(", "").replace(")", "").split(" ")
      Coordinate(coordinates(0).toDouble, coordinates(1).toDouble)
    })

    // find pois in Vienna
    val poiVienna = filterCoordinates(poiCleanCoordinates, 16.192851, 16.593533, 48.104194, 48.316388).sample(withReplacement = false, fraction = 0.001, seed = 0).persist()
    val keys = poiVienna.keys.collect()
    //val keys = poiCleanCoordinates.keys.collect()

    // writer POIs in Vienna to file
    // getTriples(keys, dataRDD, spark, viennaTriplesWriter)
    // find all the categories of pois in Vienna
    val poiFlatCategories = dataRDD.filter(x => x.getPredicate.toString().equalsIgnoreCase(conf.dataset.categoryPOI))
    val poiCategoriesVienna = poiFlatCategories.filter(x => keys.contains(x.getSubject.toString().replace(conf.dataset.poiPrefix, "").toLong))

    // from 'Node' to (poi_id, category_id) pairs, possible with duplicated keys
    val poiRawCategoriesVienna = poiCategoriesVienna.map(x => (
      x.getSubject.toString().replace(conf.dataset.poiPrefix, "").toLong,
      x.getObject.toString().replace(conf.dataset.termPrefix, "").toLong))

    // get the categories for each poi in Vienna
    val poiCategorySetVienna = poiRawCategoriesVienna.groupByKey().map(f => (f._1, f._2.toSet)).persist()
    profileWriter.println(s"Number of sampled poi in Vienna, with categories: ${poiCategorySetVienna.count()}")
    // aggregate category values based on category id
    val categoryIdValues = getCategoryValues(spark, dataRDD, conf).persist()
    profileWriter.println(s"Number of categories: ${categoryIdValues.count()}")
    val poiViennaCategoryIds = poiCategorySetVienna.flatMap(f => f._2).collect().toSet
    profileWriter.println(s"Number of categories in Vienna: ${poiViennaCategoryIds.size}")
    val categoryViennaIdValues = categoryIdValues.filter(f => poiViennaCategoryIds.contains(f._1)).persist()
    profileWriter.println(s"Number of categories with value in Vienna: ${categoryViennaIdValues.count()}")
    val pois = generatePois(spark, poiVienna, categoryViennaIdValues, poiCategorySetVienna).persist()
    profileWriter.println(s"number of poi: ${pois.count()}")

    val t1 = System.nanoTime()
    profileWriter.println("Elapsed time preparing data: " + (t1 - t0)/1000000000 + "s")

    // one hot encoding
    val oneHotDF = new Encoder().oneHotEncoding(poiCategorySetVienna, spark)
    val oneHotClusters = new Kmeans().kmClustering(numClusters = 10, df = oneHotDF, spark = spark)
    writeClusteringResult(spark.sparkContext, oneHotClusters, pois, oneHotKMFileWriter)
    val t2 = System.nanoTime()
    profileWriter.println("Elapsed time one hot: " + (t2 - t0)/1000000000 + "s")

    // word2Vec encoding
    val avgVectorDF = new Encoder().wordVectorEncoder(poiCategorySetVienna, spark)
    val avgVectorClusters = new Kmeans().kmClustering(numClusters = 10, df = avgVectorDF, spark = spark)
    writeClusteringResult(spark.sparkContext, avgVectorClusters, pois, word2VecKMFileWriter)
    val t3 = System.nanoTime()
    profileWriter.println("Elapsed time word2Vec: " + (t3 - t0)/1000000000 + "s")

    // pic clustering, build ((sid, ()), (did, ())) RDD
    val pairwisePOICategorySet = poiCategorySetVienna.cartesian(poiCategorySetVienna).filter { case (a, b) => a._1 < b._1 }
    // from ((sid, ()), (did, ())) to (sid, did, similarity)
    val pairwisePOISimilarity = pairwisePOICategorySet.map(x => (x._1._1.toLong, x._2._1.toLong,
      new Distances().jaccardSimilarity(x._1._2, x._2._2))).persist()
    val picDistanceMatrix = DistanceMatrix(pairwisePOISimilarity.map(x => Distance(x._1, x._2, 1-x._3)).collect().toList)
    Serialization.writePretty(picDistanceMatrix, picDistanceMatrixWriter)
    val clustersPIC = new PIC().picSparkML(pairwisePOISimilarity, 10, 5, spark)
    writeClusteringResult(spark.sparkContext, clustersPIC, pois, picFileWriter)
    val t4 = System.nanoTime()
    profileWriter.println("Elapsed time cartesian: " + (t4 - t0)/1000000000 + "s")

    //// distance RDD, from (sid, did, similarity) to (sid, did, distance)
    val distancePairs = pairwisePOISimilarity.map(x => (x._1, x._2, 1.0 - x._3)).persist()
    val mdsDF = new Encoder().mdsEncoding(distancePairs = distancePairs, poiCategorySetVienna.count().toInt, dimension = 2, spark = spark)
    val mdsClusters = new Kmeans().kmClustering(numClusters = 10, df = mdsDF, spark = spark)
    writeClusteringResult(spark.sparkContext, mdsClusters, pois, mdsKMFileWriter)
    val t5 = System.nanoTime()
    profileWriter.println("Elapsed time mds: " + (t5 - t0)/1000000000 + "s")

    // dbscan clustering, TODO solve scala version flicts with SANSA
    // dbscanClustering(coordinates, spark)
    // stop spark session
    // viennaTriplesWriter.close()
    picFileWriter.close()
    oneHotKMFileWriter.close()
    mdsKMFileWriter.close()
    word2VecKMFileWriter.close()
    profileWriter.close()
    spark.stop()

  }
}
