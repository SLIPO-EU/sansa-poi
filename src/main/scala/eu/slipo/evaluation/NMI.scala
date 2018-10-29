package eu.slipo.evaluation

import eu.slipo.datatypes.Clusters

class NMI {

  /**
    * Calculate Normalized Mutual Information
    */
  def calNMI(clusters: Clusters): Double ={
    val canonicalString = new CanonicalString()
    // each cluster contains a list of pair (string, int) - (canonicalCategory, number)
    val clusterPoiCanonical = clusters.clusters.map(cluster => {
      cluster.poi_in_cluster.map(poi => {
        canonicalString.generateCanonicalString(poi.categories.categories.toList)
      }).groupBy(identity).map(category => (category._1, category._2.length))
    })
    val categories = mergeMap(clusterPoiCanonical)((v1, v2) => v1 + v2)
    val num_poi = clusters.clusters.map(_.poi_in_cluster.length).sum
    val mutual_info = calMI(clusterPoiCanonical, categories, num_poi)
    val classEntropy = calClassEntropy(clusters, num_poi)
    val categoryEntropy = calCategoryEntropy(categories, num_poi)
    mutual_info / ((classEntropy + categoryEntropy) / 2)
  }


  /**
    * Calculate Mutual Information
    */
  def calMI(clusterPoiCanonical: List[Map[String, Int]], categories: Map[String, Int], num_pois: Int): Double ={
    var mutual_info = 0.0
    clusterPoiCanonical.foreach(cluster => {
        val w_i_card = cluster.size
        categories.foreach(category => {
          // number of poi with category
          val c_j_card = category._2
          // if category in cluster
          if(cluster.contains(category._1)){
            // number of category in cluster
            val intersect_w_c = cluster.get(category._1).head
            mutual_info += intersect_w_c / num_pois * math.log( (num_pois * intersect_w_c) / (w_i_card * c_j_card) )
          }
        })
      }
    )
    mutual_info
  }

  /**
    * Calculate class entropy
    */
  def calClassEntropy(clusters: Clusters, num_pois: Int): Double ={
    var omega = 0.0
    clusters.clusters.foreach(cluster => {
      val temp = cluster.poi_in_cluster.length / num_pois
      omega += temp * math.log(temp)
    })
    omega
  }

  /**
    * Calculate category entropy
    */
  def calCategoryEntropy(categories: Map[String, Int], num_pois: Int): Double ={
    var omega = 0.0
    categories.foreach(category => {
      val temp = category._2 / num_pois
      omega += temp * math.log(temp)
    })
    omega
  }

  /**
    * Merge a list of map
    * Reference https://stackoverflow.com/questions/1262741/scala-how-to-merge-a-collection-of-maps
    * @param ms
    * @param f
    * @tparam A
    * @tparam B
    * @return
    */
  def mergeMap[A, B](ms: List[Map[A, B]])(f: (B, B) => B): Map[A, B] = (Map[A, B]() /: (for (m <- ms; kv <- m) yield kv)) {
      (a, kv) => a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else kv)
  }
}
