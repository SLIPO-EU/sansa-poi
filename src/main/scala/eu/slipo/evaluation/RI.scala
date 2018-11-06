package eu.slipo.evaluation

import eu.slipo.datatypes.Clusters

class RI(clusters: Clusters) {

  /**
    * Calculate Rand index
    */
  def calRandInformationFScore(): (Double, Double, Double, Double, Double) ={
    val canonicalString = new CanonicalString()

    // all pair of pois that are clustered into each cluster
    var tp_fp = 0
    clusters.clusters.foreach(cluster => {
      tp_fp += choose(cluster.poi_in_cluster.length, 2)
    })
    println("tp_fp: ", tp_fp)

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
          tp += choose(category._2, 2)
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
    val ri = (tp + tn).toDouble / (tp + tn + fp + fn)
    (ri, tp, fp, tn, fn)
  }

  /**
    * choose k from n
    * @param n
    * @param k
    * @return
    */
  def choose(n: Int, k: Int): Int ={
    if (k == 0 || k == n){
      1
    }
    else{
      choose(n - 1, k - 1) + choose(n - 1, k)
    }
  }
}
