package eu.slipo.algorithms

import eu.slipo.data.DataTest
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.apache.spark.ml.linalg.DenseVector

class EncoderTest extends FunSuite{
  val mdsTestData = List((1.toLong, 2.toLong, 0.5), (1.toLong, 3.toLong, 1.0), (2.toLong, 3.toLong, 1.0))
  val mdsTestDataRDD: RDD[(Long, Long, Double)] = DataTest.spark.sparkContext.parallelize(mdsTestData)
  val oneHotTestData = List((1.toLong, Set("a", "b")), (2.toLong, Set("b", "c")), (3.toLong, Set("b", "d")))
  val oneHotTestDataRDD: RDD[(Long, Set[String])] = DataTest.spark.sparkContext.parallelize(oneHotTestData)


  test("Encoder.mdsEncoding"){
    val (mdsEncodedDF, mdsEncoded) = new Encoder().mdsEncoding(mdsTestDataRDD, 3, 2, DataTest.spark)
    assert(mdsEncodedDF.head().getAs[DenseVector](mdsEncodedDF.head().length-1).size === 2) // (x, y) coordinate
  }

  test("Encoder.oneHotEncoding"){
    val (oneHotEncodedDF, oneHotEncoded) = new Encoder().oneHotEncoding(oneHotTestDataRDD, DataTest.spark)
    assert(oneHotEncodedDF.head().getAs[DenseVector](oneHotEncodedDF.head().length-1).size === 4) // encoded vector
  }

  test("Encoder.wordVectorEncoder"){
    val (word2VecEncodedDF, word2VecEncoded) = new Encoder().wordVectorEncoder(oneHotTestDataRDD, DataTest.spark)
    assert(word2VecEncodedDF.head().getAs[DenseVector](word2VecEncodedDF.head().length-1).size >= 1) // vector size for poi should be larger than equal to 1
  }

}
