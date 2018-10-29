package eu.slipo.evaluation

import eu.slipo.datatypes.Clusters

class Purity {

  /**
    * Calculate purity of given clustering result
    */
  def calPurity(clusterResults: Clusters): Double ={
    val clusters = clusterResults.clusters
    val cS = new CanonicalString()
    // map each poi in cluster to canonical string representation
    val clusterCategories: List[Array[String]]  = clusters.map(f => {
      f.poi_in_cluster.map(ff => {
        cS.generateCanonicalString(ff.categories.categories.toList)
      })
    })
    // get the most number of common canonical form from each cluster
    val clusterLabelNum: List[Array[Int]] = clusterCategories.map(f => f.groupBy(identity).map(ff => ff._2.length).toArray)
    // calculate purity
    var correctLabeled = 0
    var totalPoi = 0
    clusterLabelNum.foreach(f => {
      correctLabeled += f.max
      totalPoi += f.sum
    })
    correctLabeled / totalPoi
  }
}
