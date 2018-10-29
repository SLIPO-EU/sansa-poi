package eu.slipo.evaluation

import eu.slipo.datatypes.Clusters

class RI {

  /**
    * Calculate Rand index
    * @param clusters clustering result
    */
  def calRandInformationFScore(clusters: Clusters): (Double, Double, Double, Double, Double) ={
    val canonicalString = new CanonicalString()

    // all pair of pois that are clustered into each cluster
    var tp_fp = 0
    clusters.clusters.foreach(cluster => {
      tp_fp += calComb(cluster.poi_in_cluster.length, 2)
    })

    // pair of same category poi that are assigned into same cluster
    var tp = 0
    val clusterPoiCanonical = clusters.clusters.map(cluster => {
      cluster.poi_in_cluster.map(poi => {
        canonicalString.generateCanonicalString(poi.categories.categories.toList)
      }).groupBy(identity).map(category => (category._1, category._2.length))
    })
    clusterPoiCanonical.foreach(cluster => {
      cluster.foreach(category => {
        if(category._2 > 1){
          tp += calComb(category._2, 2)
        }
      })
    })

    val fp = tp_fp - tp

    var fn_tn = 0
    var fn = 0
    for((cluster, i) <- clusterPoiCanonical.zipWithIndex){
      // get the most common category
      var mostCommonCategory1 = cluster.maxBy(f => f._2)._1
      // loop through each (category, cateogry_num)
      cluster.foreach(f => {
        // check the rest clusters
        for((cluster2, j) <- clusterPoiCanonical.zipWithIndex){
          if(j > i){ // after current cluster
            var mostCommonCategory2 = cluster2.maxBy(ff => ff._2)._1
            fn_tn = cluster.size * cluster2.size  // pair of pois not assigned into same cluster
            if(cluster2.contains(f._1)){ // if category in other clusters after current cluster contain this category
              // if category is correct label for current cluster or cluster2
              if(f._1 == mostCommonCategory1 || f._1 == mostCommonCategory2) {
                fn += f._2 * cluster2.get(f._1).head
              }
            }
          }
        }
      })
    }
    val tn = fn_tn - fn
    val ri = (tp + tn) / (tp + tn + fp + fn)
    (ri, tp, fp, tn, fn)
  }

  /**
    * Calculate combinatorial
    * @param n
    * @param k
    * @return
    */
  def calComb(n: Int, k: Int): Int ={
    //calFactorial(n) / (calFactorial(k) * calFactorial(n - k))
    0
  }

  /**
    * Calculate factorial
    * @param n
    * @return
    */
  /*def calFactorial(n: Int): Int ={
    case 0 => 1
    case _ => n * calFactorial(n - 1)
  }*/
}
