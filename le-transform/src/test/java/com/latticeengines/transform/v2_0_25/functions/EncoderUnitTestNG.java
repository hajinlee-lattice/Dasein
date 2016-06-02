package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class EncoderUnitTestNG {
    @Test(groups = "unit")
    public void transform() throws Exception {
        Encoder encoder = new Encoder();

        Map<Object, Long> valuesToCheck = new HashMap<Object, Long>();
        valuesToCheck.put("", 0l);
        valuesToCheck.put(0, 0l);
        valuesToCheck.put(1, 1l);
        valuesToCheck.put("NULL", 1463472903l);
        valuesToCheck.put("None", 1463472903l);
        valuesToCheck.put("null", 1463472903l);
        valuesToCheck.put(null, 1463472903l);
        valuesToCheck.put(true, 1l);
        valuesToCheck.put(false, 0l);

        // Check Data Type - long/float/double
        valuesToCheck.put(1l, 1l);
        valuesToCheck.put(1.0f, 1l);
        valuesToCheck.put(1.0, 1l);

        // Check English and other languages
        valuesToCheck.put("English", 1774572656l);
        valuesToCheck.put("English Spaces", 2639267869l);
        valuesToCheck.put("汉语", 996146472l);
        valuesToCheck.put("汉语 Spaces", 1907597669l);
        valuesToCheck.put("हिंदी", 3442654225l);
        valuesToCheck.put("हिंदी Spaces", 4234684988l);

        Map<String, Object> args = new HashMap<>();
        args.put("column", "StringField");

        for (Object key : valuesToCheck.keySet()) {
            Map<String, Object> record1 = new HashMap<>();
            record1.put("StringField", key);
            Object result1 = encoder.transform(args, record1);

            assertEquals(result1, valuesToCheck.get(key));
            assertEquals(result1.getClass(), Long.class);
        }
    }
}
