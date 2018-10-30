package eu.slipo.utils

import java.io._

import org.json4s.native.JsonMethods._
import eu.slipo.datatypes.{Cluster, Clusters}
import org.json4s.DefaultFormats

object JsonToCsv {

    private final val CSV_DILEMITER: String = ","

    def readJson(jsonFilePath: String, csvFilePath: String): Unit ={
      val stream = new FileInputStream(jsonFilePath)
      val stringFromJson = stringBuilder(stream)
      println("String from File: " + stringFromJson)
      val json = try {  parse(stringFromJson)} finally { stream.close() }
      //print("Json File: " + json)
      implicit val formats = DefaultFormats
      val clustersJson = json.extract[Clusters]
      clusterToCsv(clustersJson.clusters.head, csvFilePath)
    }

    def clusterToCsv(cluster: Cluster, csvPath: String): Unit ={
      try{
        val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvPath), "UTF-8"))
        // write header to csv
        val headerLine = new StringBuffer()
        headerLine.append("PoiID")
        headerLine.append(CSV_DILEMITER)
        headerLine.append("Latitude")
        headerLine.append(CSV_DILEMITER)
        headerLine.append("Longitude")
        headerLine.append(CSV_DILEMITER)
        headerLine.append("Categories")
        bw.write(headerLine.toString)
        bw.newLine()
        // write for each poi
        cluster.poi_in_cluster.foreach(poi => {
          val oneLine = new StringBuffer()
          oneLine.append(poi.poi_id)
          oneLine.append(CSV_DILEMITER)
          oneLine.append(poi.coordinate.latitude)
          oneLine.append(CSV_DILEMITER)
          oneLine.append(poi.coordinate.longitude)
          oneLine.append(CSV_DILEMITER)
          oneLine.append(poi.categories.categories.mkString(";"))
          bw.write(oneLine.toString)
          bw.newLine()
        })
        bw.flush()
        bw.close()
      }catch {
        case ioe: IOException =>
        case e: Exception =>
      }
    }

    def stringBuilder(fis: FileInputStream): String ={
      val br = new BufferedReader( new InputStreamReader(fis, "UTF-8"))
      val sb: StringBuilder = new StringBuilder
      var line: String = null
      while ( {
        line = br.readLine()
        //println(line)
        line != null
      }) {
        sb.append(line)
        sb.append('\n')
      }
      sb.toString()
    }


    def main(args: Array[String]): Unit ={
      readJson("results/pic_clusters.json", "results/pic_csv.csv")
    }
}
