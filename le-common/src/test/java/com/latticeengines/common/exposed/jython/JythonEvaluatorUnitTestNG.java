package com.latticeengines.common.exposed.jython;

import static org.testng.Assert.assertEquals;

import java.math.BigInteger;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class JythonEvaluatorUnitTestNG {

    private JythonEvaluator jythonEvaluator;

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        jythonEvaluator = JythonEvaluator.fromResource("com/latticeengines/common/exposed/jython/test_functions.py");
    }

    @Test(groups = "unit")
    public void executeCastAsString() throws Exception {
        String result = jythonEvaluator.function("castAsString", String.class, 25);
        assertEquals(result, "25");
    }

    @Test(groups = "unit")
    public void executeAdd() throws Exception {
        Integer result = jythonEvaluator.function("add", Integer.class, 3, 4);
        assertEquals(result.intValue(), 7);
    }

    @Test(groups = "unit")
    public void executeTransform() throws Exception {
        BigInteger result = jythonEvaluator.function("transform", BigInteger.class, "abcdef");
        assertEquals(result.longValue(), 2534611139L);
    }
}
