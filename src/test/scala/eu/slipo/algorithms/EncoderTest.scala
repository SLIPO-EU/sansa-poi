package eu.slipo.algorithms

import breeze.linalg
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}

class EncoderTest extends FunSuite{
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
  val oneHotTestData = List((1.toLong, Set("a", "b")), (2.toLong, Set("b", "c")), (3.toLong, Set("b", "d")))
  val oneHotTestDataRDD: RDD[(Long, Set[String])] = spark.sparkContext.parallelize(oneHotTestData)


  test("Encoder.mdsEncoding"){
    val (mdsEncodedDF, mdsEncoded) = new Encoder().mdsEncoding(mdsTestDataRDD, 3, 2, spark)
    assert(mdsEncodedDF.head().getAs[DenseVector](mdsEncodedDF.head().length-1).size == 2) // (x, y) coordinate
  }

  test("Encoder.oneHotEncoding"){
    val (oneHotEncodedDF, oneHotEncoded) = new Encoder().oneHotEncoding(oneHotTestDataRDD, spark)
    assert(oneHotEncodedDF.head().getAs[DenseVector](oneHotEncodedDF.head().length-1).size == 4) // encoded vector
  }

  test("Encoder.wordVectorEncoder"){
    val (word2VecEncodedDF, word2VecEncoded) = new Encoder().wordVectorEncoder(oneHotTestDataRDD, spark)
    assert(word2VecEncodedDF.head().getAs[DenseVector](word2VecEncodedDF.head().length-1).size >= 1) // vector size for poi should be larger than equal to 1
  }

}
