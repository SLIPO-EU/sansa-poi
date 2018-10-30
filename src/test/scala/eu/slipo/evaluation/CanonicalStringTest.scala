package eu.slipo.evaluation

import org.scalatest.FunSuite

class CanonicalStringTest extends FunSuite {
  test("CanonicalString.generateCanonicalString"){
    assert(new CanonicalString().generateCanonicalString(List("one", "two")) === "one;two")
  }
}
