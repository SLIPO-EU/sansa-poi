package eu.slipo.data

import org.apache.spark.sql.SparkSession

object DataTest {
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
  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executor.memory", "20g")
    .config("spark.driver.memory", "20g")
    .config("spark.driver.maxResultSize", "15g")
    .config("spark.ui.port", 36000)
    .config("spark.executor.cores", 4)
    .config("spark.executor.heartbeatInterval", 10000000)
    .config("spark.network.timeout", 10000001)
    .appName("PIC Cluster Test")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
}
