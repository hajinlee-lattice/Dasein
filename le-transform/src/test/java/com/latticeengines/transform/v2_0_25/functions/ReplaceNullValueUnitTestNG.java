package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class ReplaceNullValueUnitTestNG {
    @Test(groups = "unit")
    public void transform() throws Exception {
        URL url = ClassLoader.getSystemResource("com/latticeengines/transform/v2_0_25/functions/replacenullvalue");
        ReplaceNullValue replaceNullValue = new ReplaceNullValue(url.getFile());

        Map<Object, Object> valuesToCheck = new HashMap<Object, Object>();
        valuesToCheck.put(25.0, 25.0);
        valuesToCheck.put("a", "a");
        valuesToCheck.put(true, true);
        valuesToCheck.put(null, -2088190.1666666665);

        Map<String, Object> args = new HashMap<>();
        args.put("column", "AssetsStartOfYear");

        for (Object key : valuesToCheck.keySet()) {
            Map<String, Object> record1 = new HashMap<>();
            record1.put("AssetsStartOfYear", key);
            Object result1 = replaceNullValue.transform(args, record1);
            assertEquals(result1, valuesToCheck.get(key));
            if (result1 != null)
                assertEquals(result1.getClass(), valuesToCheck.get(key).getClass());
        }
    }
}
