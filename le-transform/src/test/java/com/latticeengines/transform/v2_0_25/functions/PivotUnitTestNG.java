package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class PivotUnitTestNG {

    @Test(groups = "unit")
    public void transform() throws Exception {
        URL url = ClassLoader.getSystemResource("com/latticeengines/transform/v2_0_25/functions/pivot");
        Pivot pivot = new Pivot(url.getFile());

        Map<String, Object> args = new HashMap<>();
        args.put("column1", "ModelAction1");
        args.put("column2", "ModelAction1___ISNULL__");

        Map<String, Object> record1 = new HashMap<>();
        record1.put("ModelAction1", null);
        Object result1 = pivot.transform(args, record1);
        assertEquals(result1, 1.0);

        Map<String, Object> record2 = new HashMap<>();
        record2.put("ModelAction1", "60");
        Object result2 = pivot.transform(args, record2);
        assertEquals(result2, 0.0);
    }
}
