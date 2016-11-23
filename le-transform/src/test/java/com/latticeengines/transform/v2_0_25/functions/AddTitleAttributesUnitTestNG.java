package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class AddTitleAttributesUnitTestNG {
    @Test(groups = "unit")
    public void transform() throws Exception {
        URL url = ClassLoader.getSystemResource("com/latticeengines/transform/v2_0_25/functions/addTitleAttributes");
        AddTitleAttributes addTitleAttributes = new AddTitleAttributes(url.getFile());

        Map<Object, Object> valuesToCheck = new HashMap<Object, Object>();
        valuesToCheck.put("CEO", 3.0);
        valuesToCheck.put("", 16.74705882352941);
        valuesToCheck.put(null, 16.74705882352941);
        valuesToCheck.put("AStringThatIsLongerThanThirtyCharacters", 30.0);

        Map<String, Object> args = new HashMap<>();
        args.put("column1", "Title");
        args.put("column2", "DS_TitleLength");

        for (Object key : valuesToCheck.keySet()) {
            Map<String, Object> record1 = new HashMap<>();
            record1.put("Title", key);
            Object result1 = addTitleAttributes.transform(args, record1);
            assertEquals(result1, valuesToCheck.get(key));
            if (result1 != null)
                assertEquals(result1.getClass(), valuesToCheck.get(key).getClass());
        }
    }
}
