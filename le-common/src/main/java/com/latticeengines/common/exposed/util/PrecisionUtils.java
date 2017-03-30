package com.latticeengines.common.exposed.util;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public class PrecisionUtils {

    private static final int standardPrecision = 10;

    public static final double setPrecision(double x, int precision) {
        MathContext context = new MathContext(precision, RoundingMode.HALF_UP);
        return BigDecimal.valueOf(x).round(context).doubleValue();
    }

    public static final double setPlatformStandardPrecision(double x) {
        return setPrecision(x, standardPrecision);
    }
}
