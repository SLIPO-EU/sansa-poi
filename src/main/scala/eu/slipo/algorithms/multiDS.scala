package eu.slipo.algorithms

import org.apache.spark.rdd._
import smile.mds.MDS

class multiDS {
    
    /*
     * Multi-dimensional scaling
     * 
     * Generate n dimensional coordinates based on input similarity matrix
     * */
    def multiDimensionScaling(distancePairs: RDD[(Long, Long, Double)], numPOIS: Int, dimension: Int): Array[Array[Double]] = {
      // vector keep recorded poi
      var vector = Array.ofDim[Long](numPOIS)
      // positive symmetric distance matrix
      var distanceMatrix = Array.ofDim[Double](numPOIS, numPOIS)
      // initialize distance matrix
      for (i <- 0 to numPOIS-1) {
         vector(i) = 0
         for ( j <- 0 to numPOIS-1) {
            distanceMatrix(i)(j) = 0.0
         }
      }
      var i = 0
      distancePairs.collect().foreach(x => {
                                          if(!vector.contains(x._1)){ // if there is no record for this poi
                                            vector(i) = x._1
                                            i += 1
                                          }
                                          if(!vector.contains(x._2)){ // if there is no record for this poi
                                            vector(i) = x._2
                                            i += 1
                                          }
                                          val i1 = vector.indexOf(x._1) // get the index as x-y axis for matrix
                                          val i2 = vector.indexOf(x._2) // get the index as x-y axis for matrix
                                          distanceMatrix(i1)(i2) = x._3;
                                          distanceMatrix(i2)(i1) = x._3;
                                          })
      // create coordinates
      val mds = new MDS(distanceMatrix, dimension, true)
      mds.getCoordinates
    }
    
}