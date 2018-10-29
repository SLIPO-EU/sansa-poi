package eu.slipo.evaluation

class CanonicalString {

  /**
    * Generate canonical string from a list of strings
    * @param strings a set of strings
    * @return canonical string from a list of strings
    */
  def generateCanonicalString(strings: List[String]): String ={
    strings.sorted.mkString(";")
  }
}
