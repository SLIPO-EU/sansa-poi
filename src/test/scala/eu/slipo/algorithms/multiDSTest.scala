package eu.slipo.algorithms

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import eu.slipo.data.DataTest

class multiDSTest extends FunSuite{
  val testData = List((1.toLong, 2.toLong, 0.5), (1.toLong, 3.toLong, 1.0), (2.toLong, 3.toLong, 1.0))
  val testDataRDD: RDD[(Long, Long, Double)] = DataTest.spark.sparkContext.parallelize(testData)

  test("multiDS.multiDimensionScaling"){
    val coordinates = new multiDS().multiDimensionScaling(testDataRDD, 3, 2)
    assert(coordinates.length === 3 && coordinates.head._2.length === 2)
  }
}
