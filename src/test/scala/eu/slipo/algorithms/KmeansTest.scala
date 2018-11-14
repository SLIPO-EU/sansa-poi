package eu.slipo.algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class KmeansTest extends FunSuite{
  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executor.memory", "20g")
    .config("spark.driver.memory", "20g")
    .config("spark.driver.maxResultSize", "15g")
    .config("spark.ui.port", 36000)
    .config("spark.executor.cores", 4)
    .config("spark.executor.heartbeatInterval", 10000000)
    .config("spark.network.timeout", 10000001)
    .appName("PIC Cluster Test")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val mdsTestData = List((1.toLong, 2.toLong, 0.5), (1.toLong, 3.toLong, 1.0), (2.toLong, 3.toLong, 1.0))
  val mdsTestDataRDD: RDD[(Long, Long, Double)] = spark.sparkContext.parallelize(mdsTestData)

  test("Kmeans.mdsEncoding"){
    val (mdsEncodedDF, mdsEncoded) = new Encoder().mdsEncoding(mdsTestDataRDD, 3, 2, spark)
    val km_result = new Kmeans().kmClustering(2, 2, mdsEncodedDF, spark)
    assert(km_result.size == 2) // 2 clusters
  }
}
