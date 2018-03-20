package eu.slipo.algorithms

import org.apache.spark.mllib.clustering.PowerIterationClustering
import net.sansa_stack.ml.spark.clustering.{ RDFGraphPICClustering => RDFGraphPICClusteringAlg }

class PIC {
  
  /*
   * Power Iteration clustering algorithm from Spark standard library
   * */
  def picSparkML(pairwisePOISimilarity: RDD[(Long, Long, Double)], numCentroids: Int, numIterations: Int, sparkSession: SparkSession) = {
      val model = new PowerIterationClustering().setK(numCentroids).setMaxIterations(numIterations).setInitializationMode("degree").run(pairwisePOISimilarity)
      val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
      clusters
  }
  
  /* Power Iteration using implementation from SANSA
   * */
  def picSANSA() {
    
  }
  
}