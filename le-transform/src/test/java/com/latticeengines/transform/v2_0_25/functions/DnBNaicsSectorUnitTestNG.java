package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DnBNaicsSectorUnitTestNG {

    @Test(groups = "unit", dataProvider = "naicsCodeData")
    public void transform(String naicsCode, String sectorNaicsCode, boolean isNull) {
        Map<String, Object> record = new HashMap<>();
        record.put("LE_NAICS_CODE", naicsCode);
        
        Map<String, Object>  args = new HashMap<>();
        args.put("column", "LE_NAICS_CODE");
        
        DnBNaicsSector dnbNaicsSector = new DnBNaicsSector();
        String result = (String) dnbNaicsSector.transform(args, record);
        
        if (!isNull) {
            assertEquals(result.length(), 2);
            assertEquals(result, sectorNaicsCode);
        } else {
            assertNull(result);
        }
    }
    
    @DataProvider(name = "naicsCodeData")
    public Object[][] naicsCodeData() {
        return new Object[][] { //
            new Object[] { "621111", "62", false }, //
            new Object[] { "  621111  ", "62", false }, //
            new Object[] { "6", null, true }, //
            new Object[] { null, null, true }, //
        };
    }
}
