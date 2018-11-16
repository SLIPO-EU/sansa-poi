package eu.slipo.evaluation

import eu.slipo.datatypes.Clusters
import eu.slipo.data.DataTest
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods
import org.scalatest.FunSuite

class RITest extends FunSuite{

  val clusters_f: org.json4s.JValue = JsonMethods.parse(DataTest.clustersJson)
  implicit val formats: org.json4s.DefaultFormats = DefaultFormats
  val clusters: Clusters = clusters_f.extract[Clusters]

  val RIObject = new RI(clusters)

  test("RI.calRandInformationFScore"){
    assert(RIObject.calRandInformationFScore()._1 === 1.0)
  }

  test("RI.choose"){
    assert(RIObject.choose(4, 3) === 4)
  }

}
