package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class TitleLength implements RealTimeTransform {

    public TitleLength(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object value = record.get(column);

        if(value == null)
            return 0;

        return value.toString().length();
    }
}
