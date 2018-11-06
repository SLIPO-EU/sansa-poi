package eu.slipo.evaluation

import eu.slipo.datatypes.Clusters
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods
import org.scalatest.FunSuite

class NMITest extends FunSuite{
  val clustersJson = """{
                         "numOfClusters" : 1,
                         "clusterSizes" : [ 2 ],
                         "clusters" : [
                           {
                             "cluster_id" : 1,
                             "poi_in_cluster" : [
                               {
                               "poi_id" : 1,
                               "coordinate" : {
                               "longitude" : 1.0,
                               "latitude" : 1.0
                               },
                               "categories" : {
                               "categories" : [ "test1", "test2"]
                                },
                               "review" : 1.0
                               },
                               {
                               "poi_id" : 2,
                               "coordinate" : {
                               "longitude" : 2.0,
                               "latitude" : 2.0
                               },
                               "categories" : {
                               "categories" : [ "test1", "test2"]
                               },
                               "review" : 1.0
                               }
                             ]
                           }
                         ]
                       }
                       """
  val clusters_f: org.json4s.JValue = JsonMethods.parse(clustersJson)
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
