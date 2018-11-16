package eu.slipo.utils

import eu.slipo.datatypes.{Cluster, Clusters}
import eu.slipo.data.DataTest
import org.json4s.DefaultFormats
import org.scalatest.FunSuite
import org.json4s.native.JsonMethods
import java.nio.file.{Paths, Files}

class JsonToCsvTest extends FunSuite {
  val clusters: org.json4s.JValue = JsonMethods.parse(DataTest.clustersJson)
  implicit val formats: org.json4s.DefaultFormats = DefaultFormats
  val cluster: Cluster = clusters.extract[Clusters].clusters.head

  test("JsonToCsv.clusterToCsv"){
    JsonToCsv.clusterToCsv(cluster, "/tmp/test.csv")
    assert(Files.exists(Paths.get("/tmp/test.csv")))
  }
}
