package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

public class PrecisionUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testPrecision() {
        List<Double> valuesToTest = new ArrayList<>();
        valuesToTest.add(3.14159265358979323846264338327950);
        valuesToTest.add(-3.14159265358979323846264338327950);
        valuesToTest.add(2.71828182846e15);
        valuesToTest.add(-2.71828182846e15);
        valuesToTest.add(2.71828182846e-15);
        valuesToTest.add(0.00057615364);
        valuesToTest.add(24756.0);

        List<Double> valuesAtPrecision3 = new ArrayList<>();
        valuesAtPrecision3.add(3.14);
        valuesAtPrecision3.add(-3.14);
        valuesAtPrecision3.add(2.72e15);
        valuesAtPrecision3.add(-2.72e15);
        valuesAtPrecision3.add(2.72e-15);
        valuesAtPrecision3.add(5.76e-4);
        valuesAtPrecision3.add(2.48e4);

        for (int i = 0; i < valuesToTest.size(); i++) {
            Double value0 = valuesToTest.get(i);
            Double check = valuesAtPrecision3.get(i);
            Double value1 = PrecisionUtils.setPrecision(Double.valueOf(value0.toString()), 3);
            assertEquals(value1, check);
        }
    }

}
