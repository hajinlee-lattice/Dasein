package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class PivotV2UnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(PivotV2UnitTestNG.class);

    @Test(groups = "unit")
    public void transformSimple() throws Exception {
        URL url = ClassLoader.getSystemResource("com/latticeengines/transform/v2_0_25/functions/pivot");
        PivotV2 pivot = new PivotV2(url.getFile());

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

        Map<String, Object> args3 = new HashMap<>();
        args3.put("column1", "ModelAction1");
        args3.put("column2", "ModelAction1_60");

        Map<String, Object> record3 = new HashMap<>();
        record3.put("ModelAction1", "-2.7755575615628914e-17");
        Object result3 = pivot.transform(args3, record3);
        assertEquals(result3, 1.0);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "unit")
    public void transformWithParentheses() throws Exception {
        URL url = ClassLoader.getSystemResource("com/latticeengines/transform/v2_0_25/functions/pivotWithParentheses");
        PivotV2 pivot = new PivotV2(url.getFile());

        Map<String, Object> lookupMap = pivot.getLookupMap();

        List<?> valueTuple1 = (List) lookupMap.get("Adobe2");
        assertEquals(valueTuple1.get(0), "x");
        List<?> values1 = (List) valueTuple1.get(1);
        assertEquals(values1.get(0), "Adobe(2)");

        List<?> valueTuple2 = (List) lookupMap.get("Adobe1\"'\"]1");
        assertEquals(valueTuple2.get(0), "x&?)");
        List<?> values2 = (List) valueTuple2.get(1);
        assertEquals(values2.get(0), "Adobe 1 '1]");
        assertEquals(values2.get(1), "Adobe 1 '\"1");

        List<?> valueTuple3 = (List) lookupMap.get("(Adobe)");
        assertEquals(valueTuple3.get(0), "x");
        List<?> values3 = (List) valueTuple3.get(1);
        assertEquals(values3.get(0), "[Adobe]");

        List<?> valueTuple4 = (List) lookupMap.get("Adobe33");
        assertEquals(valueTuple4.get(0), "x");
        List<?> values4 = (List) valueTuple4.get(1);
        assertEquals(values4.get(0), "Adobe (3 3)");
    }
}
