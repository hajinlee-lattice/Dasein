package com.latticeengines.serviceflows.core.transforms;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.jython.JythonEngine;

public class JythonFunctionsUnitTestNG {
    
    private JythonEngine engine = new JythonEngine(null);

    @DataProvider(name = "functions")
    public Object[][] getFunctions() {
        return new Object[][] { //
                new Object[] { "title_length", "length", Integer.class, new String[] { "xyz" }, 3 } //
                
        };
    }
    
    @Test(groups = "unit", dataProvider = "functions")
    public void testFunctions(String moduleName, //
            String functionName, //
            Class<?> returnType, //
            String[] params, //
            Object expectedResult) {
        Object result = engine.invoke("com.latticeengines.serviceflows.core.transforms", //
                moduleName, functionName, params, returnType);
        assertEquals(result, expectedResult);
    }
}
