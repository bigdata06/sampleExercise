
package org.viirya.util

import org.scalatest.FunSuite

class UtilsSuite extends FunSuite {

  test("num should be greater than 0") {
    intercept[IllegalArgumentException] {
      Utils.leftTruncate(-1)
    }
  }

  test("leftTruncate with num greater than 0") {
    assert(Utils.leftTruncate(1234) === List(1234, 234, 34, 4))
    assert(Utils.leftTruncate(0) === List(0))
  }
}
