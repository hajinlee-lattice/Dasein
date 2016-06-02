package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class MakeFloatUnitTestNG {
    @Test(groups = "unit")
    public void transform() throws Exception {
        MakeFloat makeFloat = new MakeFloat();

        Map<Object, Double> valuesToCheck = new HashMap<Object, Double>();
        valuesToCheck.put(null, null);
        valuesToCheck.put(25.0, 25.0);
        valuesToCheck.put("a", null);
        valuesToCheck.put(true, null);

        Map<String, Object> args = new HashMap<>();
        args.put("column", "StringField");

        for (Object key : valuesToCheck.keySet()) {
            Map<String, Object> record1 = new HashMap<>();
            record1.put("StringField", key);
            Object result1 = makeFloat.transform(args, record1);
            assertEquals(result1, valuesToCheck.get(key));
            if (result1 != null) {
                assertEquals(result1.getClass(), Double.class);
            }
        }
    }
}
