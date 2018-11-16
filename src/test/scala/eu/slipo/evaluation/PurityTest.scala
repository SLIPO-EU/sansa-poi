package eu.slipo.evaluation

import eu.slipo.datatypes.Clusters
import eu.slipo.data.DataTest
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods
import org.scalatest.FunSuite

class PurityTest extends FunSuite{

  val clusters_f: org.json4s.JValue = JsonMethods.parse(DataTest.clustersJson)
  implicit val formats: org.json4s.DefaultFormats = DefaultFormats
  val clusters: Clusters = clusters_f.extract[Clusters]

  test("Purity.calPurity"){
    assert(new Purity(clusters).calPurity() === 1)
  }
}
