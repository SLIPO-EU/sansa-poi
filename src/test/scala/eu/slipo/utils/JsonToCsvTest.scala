package eu.slipo.utils

import eu.slipo.datatypes.{Cluster, Clusters}
import org.json4s.DefaultFormats
import org.scalatest.FunSuite
import org.json4s.native.JsonMethods
import java.nio.file.{Paths, Files}

class JsonToCsvTest extends FunSuite {

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
  val clusters: org.json4s.JValue = JsonMethods.parse(clustersJson)
  implicit val formats: org.json4s.DefaultFormats = DefaultFormats
  val cluster: Cluster = clusters.extract[Clusters].clusters.head

  test("JsonToCsv.clusterToCsv"){
    JsonToCsv.clusterToCsv(cluster, "/tmp/test.csv")
    assert(Files.exists(Paths.get("/tmp/test.csv")))
  }
}
