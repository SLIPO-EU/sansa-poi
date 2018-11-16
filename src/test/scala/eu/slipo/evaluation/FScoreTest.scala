package eu.slipo.evaluation

import eu.slipo.datatypes.Clusters
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods
import org.scalatest.FunSuite
import eu.slipo.data.DataTest

class FScoreTest extends FunSuite{
  val clusters_f: org.json4s.JValue = JsonMethods.parse(DataTest.clustersJson)
  implicit val formats: org.json4s.DefaultFormats = DefaultFormats
  val clusters: Clusters = clusters_f.extract[Clusters]

  val RIObject = new RI(clusters)
  val (ri, tp, fp, tn, fn) = RIObject.calRandInformationFScore()

  val FScoreObject = new FScore()

  test("FScore.calFScore"){
    assert(FScoreObject.calFScore(fp = fp, tp = tp, fn = fn) === 1)
  }
}
