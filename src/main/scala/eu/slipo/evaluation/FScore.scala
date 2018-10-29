package eu.slipo.evaluation

class FScore {

  /**
    * Calculate the F-Score based on the given clustering result
    */
  def calFScore(fp: Double, tp: Double, fn: Double): Double ={
    val precision = tp/(tp + fp)
    val recall = tp/(tp + fn)
    val beta = 1.5
    ((math.pow(beta, 2) + 1) * precision * recall) / (math.pow(beta, 2) * precision + recall)
  }
}
