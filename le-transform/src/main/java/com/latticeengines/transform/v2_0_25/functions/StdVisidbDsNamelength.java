package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsNamelength implements RealTimeTransform {

    public StdVisidbDsNamelength(String modelPath) {

    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column1 = (String) arguments.get("column1");
        String column2 = (String) arguments.get("column2");

        Object firstName = record.get(column1);
        Object lastName = record.get(column2);

        if (firstName == null)
            firstName = "";
        if (lastName == null)
            lastName = "";

        return firstName.toString().length() + lastName.toString().length();
    }
}
