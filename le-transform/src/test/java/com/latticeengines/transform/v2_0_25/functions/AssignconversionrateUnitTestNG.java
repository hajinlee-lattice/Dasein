package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AssignconversionrateUnitTestNG {

    @Test(groups = "unit", dataProvider = "columnValueProvider")
    public void transform(String column, Object value, double expectedResult, boolean exception) {
        URL url = ClassLoader.getSystemResource("com/latticeengines/transform/v2_0_25/functions/assignconversionrate");
        Assignconversionrate acr = new Assignconversionrate(url.getFile());
        
        Map<String, Object> args = new HashMap<>();
        args.put("column", column);

        Map<String, Object> record = new HashMap<>();
        record.put(column, value);
        
        boolean exceptionThrown = false;
        try {
            assertEquals(acr.transform(args, record), expectedResult);
        } catch (Exception e) {
            exceptionThrown = true;
        }
        assertEquals(exceptionThrown, exception);
    }
    
    @DataProvider(name = "columnValueProvider")
    private Object[][] columnValueProvider() {
        return new Object[][] { //
            { "TechIndicator_HewlettPackard", null, 1.0, false }, //
            { "TechIndicator_HewlettPackard", "Yes", 1.0, false }, //
            { "TechIndicator_HewlettPackard", "No", 0.03, false }, //
            { "TechIndicator_HewlettPackard", "null", 0.02, false }, //
            { "COLUMN_DOES_NOT_EXIST", null, -1.0, true }, //
        };
    }
}
