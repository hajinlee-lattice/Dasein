package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsFirstnameSameasLastname implements RealTimeTransform {

    public StdVisidbDsFirstnameSameasLastname(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column1 = (String) arguments.get("column1");
        String column2 = (String) arguments.get("column2");

        String firstName = column1 == null ? null : String.valueOf(record.get(column1));
        String lastName = column2 == null ? null : String.valueOf(record.get(column2));

        return calcualteStdVisidbDsFirstnameSameasLastname(firstName, lastName);
    }

    public static boolean calcualteStdVisidbDsFirstnameSameasLastname(
            String firstName, String lastName) {
        if (StringUtils.isEmpty(firstName) || StringUtils.isEmpty(lastName))
            return false;

        firstName = firstName.trim().toLowerCase();
        lastName = lastName.trim().toLowerCase();

        if (firstName.equals(lastName))
            return true;

        return false;
    }
}
