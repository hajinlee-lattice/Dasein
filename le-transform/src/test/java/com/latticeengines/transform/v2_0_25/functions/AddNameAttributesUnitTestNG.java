package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class AddNameAttributesUnitTestNG {

    private AddNameAttributes addNameAttributes;

    public AddNameAttributesUnitTestNG() {
        URL url = ClassLoader.getSystemResource("com/latticeengines/transform/v2_0_25/functions/addNameAttributes");
        addNameAttributes = new AddNameAttributes(url.getFile());
    }

    @Test(groups = "unit")
    public void transform() throws Exception {

        Map<String, Object> args = new HashMap<>();
        args.put("column1", "FirstName");
        args.put("column2", "DS_FirstName_IsNull");

        Map<Object, Object> valuesToCheck = new HashMap<Object, Object>();
        valuesToCheck.put("Bob", 0.0);
        valuesToCheck.put(null, 1.0);
        checkValues("FirstName", args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("Bob", 3.0);
        valuesToCheck.put("Suzanne", 7.0);
        valuesToCheck.put("AStringThatIsLongerThanThirtyCharacters", 30.0);
        valuesToCheck.put(null, 3.89);
        args.put("column2", "DS_FirstName_Length");
        checkValues("FirstName", args, valuesToCheck);

        args.put("column1", "LastName");
        valuesToCheck.clear();
        valuesToCheck.put("Bob", 0.0);
        valuesToCheck.put(null, 1.0);
        args.put("column2", "DS_LastName_IsNull");
        checkValues("LastName", args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("Newton", 6.0);
        valuesToCheck.put("AStringThatIsLongerThanThirtyCharacters", 30.0);
        valuesToCheck.put(null, 5.0);
        args.put("column2", "DS_LastName_Length");
        checkValues("LastName", args, valuesToCheck);

        args.clear();
        args.put("column1", "FirstName");
        args.put("column2", "LastName");
        args.put("column3", "DS_Name_Length");

        Map<Map.Entry<Object, Object>, Object> valuesToCheckFirstAndLastName = new HashMap<>();
        valuesToCheckFirstAndLastName.put(new HashMap.SimpleEntry<Object, Object>("Dan", "Brown"), 8.0);
        valuesToCheckFirstAndLastName
                .put(new HashMap.SimpleEntry<Object, Object>("Dan", "AStringThatIsLongerThanThirtyCharacters"), 33.0);
        valuesToCheckFirstAndLastName.put(new HashMap.SimpleEntry<Object, Object>(null, "Brown"), 12.0);
        valuesToCheckFirstAndLastName.put(new HashMap.SimpleEntry<Object, Object>("Dan", null), 12.0);
        checkValuesFirstAndLastName(args, valuesToCheckFirstAndLastName);

        valuesToCheckFirstAndLastName.clear();
        valuesToCheckFirstAndLastName.put(new HashMap.SimpleEntry<Object, Object>("Dan ", "Dan"), 1.0);
        valuesToCheckFirstAndLastName.put(new HashMap.SimpleEntry<Object, Object>("dan", " Dan"), 1.0);
        valuesToCheckFirstAndLastName.put(new HashMap.SimpleEntry<Object, Object>("dan", null), 0.0);
        valuesToCheckFirstAndLastName.put(new HashMap.SimpleEntry<Object, Object>(null, "Alejandro"), 0.0);
        valuesToCheckFirstAndLastName.put(new HashMap.SimpleEntry<Object, Object>("Sophia", "Alejandro"), 0.0);
        args.put("column3", "DS_FirstName_SameAsLastName");
        checkValuesFirstAndLastName(args, valuesToCheckFirstAndLastName);
    }

    private void checkValues(String attributeName, Map<String, Object> args, Map<Object, Object> valuesToCheck) {
        for (Object key : valuesToCheck.keySet()) {
            Map<String, Object> record1 = new HashMap<>();
            record1.put(attributeName, key);
            Object result1 = addNameAttributes.transform(args, record1);
            Double result1Rounded = BigDecimal.valueOf((Double) result1).setScale(6, RoundingMode.HALF_UP)
                    .doubleValue();
            assertEquals(result1, valuesToCheck.get(key));
            if (result1 != null)
                assertEquals(result1.getClass(), valuesToCheck.get(key).getClass());
        }
    }

    private void checkValuesFirstAndLastName(Map<String, Object> args,
            Map<Map.Entry<Object, Object>, Object> valuesToCheck) {
        for (Map.Entry<Object, Object> entry : valuesToCheck.keySet()) {
            Map<String, Object> record1 = new HashMap<>();
            record1.put("FirstName", entry.getKey());
            record1.put("LastName", entry.getValue());
            Object result1 = addNameAttributes.transform(args, record1);
            Double result1Rounded = BigDecimal.valueOf((Double) result1).setScale(6, RoundingMode.HALF_UP)
                    .doubleValue();
            assertEquals(result1Rounded, valuesToCheck.get(entry));
            if (result1 != null)
                assertEquals(result1.getClass(), valuesToCheck.get(entry).getClass());
        }
    }
}
