package com.latticeengines.spark.util

private[spark] object  ProfileUtils {
  def roundTo5(x: Double): Double = {
    roundTo(x, sigDigits=1)
    if (math.abs(x)<= 10) {
      roundTo(x, sigDigits=1)
    } else {
      val x2 = roundTo(x, sigDigits = 2)
      val secondDigit = getSecondDigit(x2)
      val round5Set = Set(3, 4, 5, 6, 7)
      if (round5Set.contains(secondDigit)) {
        replaceSecondDigitByFive(x2)
      } else {
        roundTo(x, sigDigits=1)
      }
    }
  }

  def roundTo(x: Double, sigDigits: Int): Double = {
    val scale: Int = getScale(x, sigDigits)
    BigDecimal(x).setScale(scale, BigDecimal.RoundingMode.HALF_EVEN).toDouble
  }

  private def getSecondDigit(x: Double): Int = {
    val scale: Int = getScale(x, sigDigits = 2)
    math.floor(math.abs(x) * math.pow(10, scale)).toInt % 10
  }

  private def replaceSecondDigitByFive(x: Double): Double = {
    val scale: Int = getScale(x, sigDigits = 2)
    val xStr = String.valueOf(math.abs(x))
    val dotPos = xStr.indexOf(".")
    val replaced = if (xStr.contains("E")) {
      // 1.7E9
      val ePos = xStr.indexOf("E")
      (xStr.substring(0, dotPos) + ".5" + xStr.substring(ePos)).toDouble
    } else {
      val dotPos = xStr.indexOf(".")
      val digitPos = dotPos + scale - 1
      (xStr.substring(0, digitPos) + "5" + xStr.substring(digitPos + 1)).toDouble
    }
    if (x >= 0) replaced else -replaced
  }

  private def getScale(x: Double, sigDigits: Int): Int = {
    val digits: Int = math.log10(math.abs(x)).toInt
    if (digits == 0) {
      if (x >= 1) -digits + (sigDigits - 1) else -digits + sigDigits
    } else {
      if (digits > 0) -digits + (sigDigits - 1) else -digits + sigDigits
    }
  }
}
