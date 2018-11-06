package eu.slipo.evaluation

import eu.slipo.datatypes.Clusters

class NMI(clusters: Clusters) {

  val canonicalString = new CanonicalString()

  /**
    * Get clusters with canonical string for each poi
    * @return clusters containing canonical string for each poi
    */
  def getClustersWithCanonicalString: List[Map[String, Int]] ={
    val clusterPoiCanonical = clusters.clusters.map(cluster => {
      cluster.poi_in_cluster.map(poi => {
        canonicalString.generateCanonicalString(poi.categories.categories.toList)
      }).groupBy(identity).map(category => (category._1, category._2.length))
    })
    clusterPoiCanonical
  }

  /**
    * Get a map with (canonical_category, num_poi_with_canonical_category)
    * @param clusterPoiCanonical clusters containing canonical string for each poi
    * @return map with (canonical_category, num_poi_with_canonical_category)
    */
  def getCategories(clusterPoiCanonical: List[Map[String, Int]]): Map[String, Int] ={
    val categories = mergeMap(clusterPoiCanonical)((v1, v2) => v1 + v2)
    categories
  }

  /**
    * Get number of poi in clusters
    * @return number of poi
    */
  def getNumPoi: Int ={
    clusters.clusters.map(_.poi_in_cluster.length).sum
  }

  /**
    * Calculate Normalized Mutual Information
    * @return nmi
    */
  def calNMI(): Double ={
    val numPoi = getNumPoi
    val clusterPoiCanonical = getClustersWithCanonicalString
    val categories = getCategories(clusterPoiCanonical)
    val mi: Double = calMI(clusterPoiCanonical, categories, numPoi)
    val classEntropy: Double = calClassEntropy(numPoi)
    val categoryEntropy: Double = calCategoryEntropy(categories, numPoi)
    // perfect match, return 1.0
    if((categories.size == 1 && clusterPoiCanonical.length == 1) || (categories.size == 1 && clusterPoiCanonical.length == 1)){
      return 1.0
    }
    mi / ((classEntropy + categoryEntropy) / 2)
  }


  /**
    * Calculate Mutual Information
    * @param clusterPoiCanonical clusters with canonical category for poi
    * @param categories canonical categories in clusters
    * @param numPoi number of poi in clusters
    * @return mi
    */
  def calMI(clusterPoiCanonical: List[Map[String, Int]], categories: Map[String, Int], numPoi: Int): Double ={
    var mutual_info = 0.0
    clusterPoiCanonical.foreach(cluster => {
        val w_i_card = cluster.foldLeft(0)(_+_._2)
        categories.foreach(category => {
          val c_j_card = category._2
          // if category in cluster
          if(cluster.contains(category._1)){
            // number of category in cluster
            val intersect_w_c = cluster.get(category._1).head
            //println("intersect: " + intersect_w_c.toString + ", category: " + category._1)
            mutual_info += intersect_w_c / numPoi.toDouble * math.log( (numPoi.toDouble * intersect_w_c) / (w_i_card * c_j_card) )
          }
        })
      }
    )
    mutual_info
  }

  /**
    * Calculate class entropy
    * @param numPoi number of poi in clusters
    * @return cluster entropy
    */
  def calClassEntropy(numPoi: Int): Double ={
    var omega = 0.0
    clusters.clusters.foreach(cluster => {
      val temp = cluster.poi_in_cluster.length / numPoi.toDouble
      omega += temp * math.log(temp)
    })
    0.0 - omega
  }

  /**
    * Calculate category entropy
    * @param categories canonical categories in clusters
    * @param numPoi number of poi in clusters
    * @return category entropy in clusters
    */
  def calCategoryEntropy(categories: Map[String, Int], numPoi: Int): Double ={
    var omega = 0.0
    categories.foreach(category => {
      val temp = category._2 / numPoi.toDouble
      omega += temp * math.log(temp)
    })
    0.0 - omega
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
