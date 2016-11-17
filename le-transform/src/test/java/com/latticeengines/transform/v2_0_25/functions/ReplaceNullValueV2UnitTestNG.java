package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class ReplaceNullValueV2UnitTestNG {
    @Test(groups = "unit")
    public void transform() throws Exception {
        URL url = ClassLoader.getSystemResource("com/latticeengines/transform/v2_0_25/functions/replacenullvalue");
        ReplaceNullValueV2 replaceNullValue = new ReplaceNullValueV2(url.getFile());

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

        Map<String, Object> args2 = new HashMap<>();
        args2.put("column", "Domain_Length");
        Map<String, Object> record2 = new HashMap<>();
        record2.put("Domain_Length", null);
        Object result2 = replaceNullValue.transform(args2, record2);
        assertEquals(result2, 12.0);

        Map<String, Object> args3 = new HashMap<>();
        args3.put("column", "CloudTechnologies_Telephony");
        Map<String, Object> record3 = new HashMap<>();
        record3.put("CloudTechnologies_Telephony", null);
        Object result3 = replaceNullValue.transform(args3, record3);
        assertEquals(result3, -2.7755575615628914e-17);

        Map<String, Object> args4 = new HashMap<>();
        args4.put("column", "CloudTechnologies_SoftwareOther");
        Map<String, Object> record4 = new HashMap<>();
        record4.put("CloudTechnologies_SoftwareOther", null);
        Object result4 = replaceNullValue.transform(args4, record4);
        assertEquals(result4, 0.2052980132450331);
    }
}
