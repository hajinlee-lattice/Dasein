package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class AddCompanyNameAttributesUnitTestNG {

    private AddCompanyNameAttributes addCompanyNameAttributes;

    public AddCompanyNameAttributesUnitTestNG() {
        URL url = ClassLoader
                .getSystemResource("com/latticeengines/transform/v2_0_25/functions/addCompanyNameAttributes");
        addCompanyNameAttributes = new AddCompanyNameAttributes(url.getFile());
    }

    @Test(groups = "unit")
    public void transform() throws Exception {

        Map<String, Object> args = new HashMap<>();
        args.put("column1", "CompanyName");
        args.put("column2", "DS_Company_Length");

        Map<Object, Object> valuesToCheck = new HashMap<Object, Object>();
        valuesToCheck.put("A Company Making Money", 22.0);
        valuesToCheck.put("", 0.0);
        valuesToCheck.put(null, 7.48);
        valuesToCheck.put("AStringThatIsLongerThanThirtyCharacters", 30.0);
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("AValue", 0.0);
        valuesToCheck.put(null, 1.0);
        args.put("column2", "DS_Company_IsNull");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("none", 1.0);
        valuesToCheck.put(null, 0.0);
        valuesToCheck.put("#", 1.0);
        valuesToCheck.put("9", 1.0);
        valuesToCheck.put("a", 0.0);
        valuesToCheck.put("GottaGo!", 1.0);
        valuesToCheck.put("asd", 1.0);
        valuesToCheck.put("XYZ", 1.0);
        args.put("column2", "DS_Company_NameHasUnusualChar");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put(null, 0.19);
        valuesToCheck.put("none", 0.25992954);
        valuesToCheck.put("#100", 0.25992954);
        valuesToCheck.put("AStringThatIsLongerThanThirtyCharacters", 0.09064897);
        valuesToCheck.put("ABA", 0.21217068);
        valuesToCheck.put("ABAABAABAABA", 0.05304280);
        args.put("column2", "DS_Company_Entropy");
        checkValues(args, valuesToCheck);
    }

    private void checkValues(Map<String, Object> args, Map<Object, Object> valuesToCheck) {
        for (Object key : valuesToCheck.keySet()) {
            Map<String, Object> record1 = new HashMap<>();
            record1.put("CompanyName", key);
            Object result1 = addCompanyNameAttributes.transform(args, record1);
            Double result1Rounded = BigDecimal.valueOf((Double) result1).setScale(8, RoundingMode.HALF_UP)
                    .doubleValue();
            assertEquals(result1Rounded, valuesToCheck.get(key));
            if (result1 != null)
                assertEquals(result1.getClass(), valuesToCheck.get(key).getClass());
        }
    }
}
