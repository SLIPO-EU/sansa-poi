package eu.slipo.algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class PICTest extends FunSuite{
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

  val testData = List((1.toLong, 2.toLong, 1.0), (1.toLong, 3.toLong, 0.0), (2.toLong, 3.toLong, 0.0))
  val testDataRDD: RDD[(Long, Long, Double)] = spark.sparkContext.parallelize(testData)

  test("PIC.picSparkML"){
    val clusters = new PIC().picSparkML(testDataRDD, 2, 1, sparkSession = spark)
    assert(clusters.size == 2)
  }
}
