package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class AddPhoneAttributesUnitTestNG {

    private AddPhoneAttributes addPhoneAttributes;

    public AddPhoneAttributesUnitTestNG() {
        URL url = ClassLoader.getSystemResource("com/latticeengines/transform/v2_0_25/functions/addPhoneAttributes");
        addPhoneAttributes = new AddPhoneAttributes(url.getFile());
    }

    @Test(groups = "unit")
    public void transform() throws Exception {

        Map<String, Object> args = new HashMap<>();
        args.put("column1", "PhoneNumber");
        args.put("column2", "DS_Phone_Entropy");

        Map<Object, Object> valuesToCheck = new HashMap<Object, Object>();
        valuesToCheck.put(null, 0.68);
        valuesToCheck.put("6505557878", 1.470808);
        valuesToCheck.put("650650650650", 1.098612);
        valuesToCheck.put("650-555-7878", 1.470808);
        valuesToCheck.put("(650) 555-7878", 1.470808);
        valuesToCheck.put("U.S.: 650-555-7878", 1.470808);
        checkValues(args, valuesToCheck);
    }

    private void checkValues(Map<String, Object> args, Map<Object, Object> valuesToCheck) {
        for (Object key : valuesToCheck.keySet()) {
            Map<String, Object> record1 = new HashMap<>();
            record1.put("PhoneNumber", key);
            Object result1 = addPhoneAttributes.transform(args, record1);
            Double result1Rounded = BigDecimal.valueOf((Double) result1).setScale(6, RoundingMode.HALF_UP)
                    .doubleValue();
            assertEquals(result1Rounded, valuesToCheck.get(key));
            if (result1 != null)
                assertEquals(result1.getClass(), valuesToCheck.get(key).getClass());
        }
    }
}
