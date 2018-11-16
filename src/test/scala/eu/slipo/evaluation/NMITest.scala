package eu.slipo.evaluation

import eu.slipo.datatypes.Clusters
import eu.slipo.data.DataTest
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods
import org.scalatest.FunSuite

class NMITest extends FunSuite{
  val clusters_f: org.json4s.JValue = JsonMethods.parse(DataTest.clustersJson)
  implicit val formats: org.json4s.DefaultFormats = DefaultFormats
  val clusters: Clusters = clusters_f.extract[Clusters]
  val nmiObject = new NMI(clusters)
  val clusterPoiCanonical: List[Map[String, Int]] = nmiObject.getClustersWithCanonicalString
  val categories: Map[String, Int] = nmiObject.getCategories(clusterPoiCanonical)
  val numPoi: Int = nmiObject.getNumPoi

  test("NMI.getClustersWithCanonicalString"){
    assert(clusterPoiCanonical.head.head._1 === "test1;test2")
  }

  test("NMI.getCategories"){
    assert(categories.get("test1;test2").head === 2)
  }

  test("NMI.getNumPoi"){
    assert(numPoi === 2)
  }

  test("NMI.calNMI"){
    assert(nmiObject.calNMI() === 1)
  }

  test("NMI.calMI"){
    assert(nmiObject.calMI(clusterPoiCanonical, categories, numPoi) === 0.0)
  }

  test("NMI.calClassEntropy"){
    assert(nmiObject.calClassEntropy(numPoi) === 0.0)
  }

  test("NMI.calCategoryEntropy"){
    assert(nmiObject.calCategoryEntropy(categories, numPoi) === 0.0)
  }

  test("NMI.mergeMap"){
    assert(nmiObject.mergeMap(clusterPoiCanonical)((v1, v2) => v1 + v2).head._1 === "test1;test2")
  }
}
