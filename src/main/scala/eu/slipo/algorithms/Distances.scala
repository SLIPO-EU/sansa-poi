package eu.slipo.algorithms

class Distances {
    
    /*
     * Jaccard Similarity Coefficient between two sets of categories corresponding to two pois
     * */
    def jaccardSimilarity(x: Set[Long], y: Set[Long]): Double = {
      val union_l = x.toList.length + y.toList.length
      val intersect_l = x.intersect(y).toList.length
      intersect_l / (union_l - intersect_l)
    }
}