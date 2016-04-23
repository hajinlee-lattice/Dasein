package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdLength implements RealTimeTransform {

    public StdLength(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        String value = column == null ? null : String.valueOf(record.get(column));

        if(value.equals("null"))
            return null;

        return calculateStdLength(value);
    }

    public static int calculateStdLength(String value) {
        if (StringUtils.isEmpty(value))
            return 1;
        if (value.trim().length() > 30)
            return 30;

        return value.trim().length();
    }
}
