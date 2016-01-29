package com.latticeengines.common.exposed.jython;

import static org.testng.Assert.assertNotNull;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class JythonEngineUnitTestNG {
    
    private Map<String, Object> arguments = new HashMap<>();
    private Map<String, Object> record = new HashMap<>();
    private JythonEngine engine;
    
    @DataProvider(name = "functions")
    public Object[][] functions() {
        return new Object[][] { //
                new Object[] { "encoder", "stringcol", "xyz", Long.class }, //
                new Object[] { "encoder", "stringcol", "my dear aunt sally", Long.class }, //
                new Object[] { "make_float", "string_must_be_float", "1234.0", Double.class }, //
                new Object[] { "returnstring", "stringcol", "", String.class }, //
                new Object[] { "returnint", "intcol", "", Integer.class }, //
                new Object[] { "replace_null_value", "AnnualRevenue", null, Double.class }, //
        };
    }

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        URL url = ClassLoader.getSystemResource("com/latticeengines/common/exposed/jython/model");
        String simulatedModelPath = url.getPath();
        engine = new JythonEngine(simulatedModelPath);
    }

    @Test(groups = "unit", dataProvider = "functions")
    public void invoke(String functionName, String column, Object value, Class<?> returnType) {
        arguments.put("column", column);
        record.put(column, value);
        Object retval = engine.invoke(functionName, arguments, record, returnType);
        assertNotNull(retval);
    }
}
