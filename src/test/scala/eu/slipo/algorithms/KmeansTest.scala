package eu.slipo.algorithms

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import eu.slipo.data.DataTest

class KmeansTest extends FunSuite{

  val mdsTestData = List((1.toLong, 2.toLong, 0.5), (1.toLong, 3.toLong, 1.0), (2.toLong, 3.toLong, 1.0))
  val mdsTestDataRDD: RDD[(Long, Long, Double)] = DataTest.spark.sparkContext.parallelize(mdsTestData)

  test("Kmeans.mdsEncoding"){
    val (mdsEncodedDF, mdsEncoded) = new Encoder().mdsEncoding(mdsTestDataRDD, 3, 2, DataTest.spark)
    val km_result = new Kmeans().kmClustering(2, 2, mdsEncodedDF, DataTest.spark)
    assert(km_result.size === 2) // 2 clusters
  }
}
