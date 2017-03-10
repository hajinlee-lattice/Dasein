package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class AddEmailAttributesUnitTestNG {

    private AddEmailAttributes addEmailAttributes;

    public AddEmailAttributesUnitTestNG() {
        URL url = ClassLoader.getSystemResource("com/latticeengines/transform/v2_0_25/functions/addEmailAttributes");
        addEmailAttributes = new AddEmailAttributes(url.getFile());
    }

    @Test(groups = "unit")
    public void transform() throws Exception {

        Map<String, Object> args = new HashMap<>();
        args.put("column1", "Email");
        args.put("column2", "DS_Email_Length");

        Map<Object, Object> valuesToCheck = new HashMap<Object, Object>();
        valuesToCheck.put("user@domain.com", 15.0);
        valuesToCheck.put("", 0.0);
        valuesToCheck.put(null, 15.3);
        valuesToCheck.put("AStringThatIsLongerThanThirtyCharacters", 30.0);
        valuesToCheck.put("usernodomain", 12.0);
        valuesToCheck.put("@", 1.0);
        valuesToCheck.put("34@", 3.0);
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("user@domain.com", 0.0);
        valuesToCheck.put("", 1.0);
        valuesToCheck.put(null, 1.0);
        valuesToCheck.put("AString@ThatIsLongerThanThirtyCharacters", 0.0);
        valuesToCheck.put("usernodomain", 1.0);
        valuesToCheck.put("@", 1.0);
        valuesToCheck.put("34@", 1.0);
        args.put("column2", "DS_Email_IsInvalid");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("user@domain.com", 4.0);
        valuesToCheck.put("", 0.0);
        valuesToCheck.put(null, 7.1);
        valuesToCheck.put("AStringThatIsLongerThanThirtyCharacters@somewhere.com", 30.0);
        valuesToCheck.put("usernodomain", 0.0);
        valuesToCheck.put("@", 0.0);
        valuesToCheck.put("4@", 1.0);
        valuesToCheck.put("ab@", 2.0);
        args.put("column2", "DS_Email_PrefixLength");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("user@domain.com", 10.0);
        valuesToCheck.put("", 0.0);
        valuesToCheck.put(null, 8.2);
        valuesToCheck.put("user@AStringThatIs@LongerThanThirtyCharacters", 30.0);
        valuesToCheck.put("usernodomain", 0.0);
        valuesToCheck.put("@", 0.0);
        valuesToCheck.put("@a", 1.0);
        valuesToCheck.put("@ab", 2.0);
        valuesToCheck.put("a@ab", 2.0);
        args.put("column2", "DS_Domain_Length");
        checkValues(args, valuesToCheck);
    }

    private void checkValues(Map<String, Object> args, Map<Object, Object> valuesToCheck) {
        for (Object key : valuesToCheck.keySet()) {
            Map<String, Object> record1 = new HashMap<>();
            record1.put("Email", key);
            Object result1 = addEmailAttributes.transform(args, record1);
            Double result1Rounded = BigDecimal.valueOf((Double) result1).setScale(8, RoundingMode.HALF_UP)
                    .doubleValue();
            assertEquals(result1Rounded, valuesToCheck.get(key));
            if (result1 != null)
                assertEquals(result1.getClass(), valuesToCheck.get(key).getClass());
        }
    }
}
