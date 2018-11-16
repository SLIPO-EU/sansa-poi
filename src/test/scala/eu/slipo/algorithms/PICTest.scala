package eu.slipo.algorithms

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import eu.slipo.data.DataTest

class PICTest extends FunSuite{


  val testData = List((1.toLong, 2.toLong, 1.0), (1.toLong, 3.toLong, 0.0), (2.toLong, 3.toLong, 0.0))
  val testDataRDD: RDD[(Long, Long, Double)] = DataTest.spark.sparkContext.parallelize(testData)

  test("PIC.picSparkML"){
    val clusters = new PIC().picSparkML(testDataRDD, 2, 1, sparkSession = DataTest.spark)
    assert(clusters.size === 2)
  }
}
