package com.latticeengines.common.exposed.jython;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.math.BigInteger;
import java.net.URL;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class JythonEvaluatorUnitTestNG {
    
    private JythonEvaluator jythonEvaluator;
    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        jythonEvaluator = new JythonEvaluator();
        URL url = ClassLoader.getSystemResource("com/latticeengines/common/exposed/jython/test_functions.py");
        File pythonFile = new File(url.getFile());
        jythonEvaluator.initialize(pythonFile.getAbsolutePath());
    }

    @Test(groups = "unit")
    public void executeCastAsString() throws Exception {
        String result = jythonEvaluator.execute("castAsString(25)", String.class);
        assertEquals(result, "25");
    }

    @Test(groups = "unit")
    public void executeAdd() throws Exception {
        Integer result = jythonEvaluator.execute("add(3,4)", Integer.class);
        assertEquals(result.intValue(), 7);
    }

    @Test(groups = "unit")
    public void executeTransform() throws Exception {
        BigInteger result = jythonEvaluator.execute("transform('abcdef')", BigInteger.class);
        assertEquals(result.longValue(), 2534611139L);
    }
}
