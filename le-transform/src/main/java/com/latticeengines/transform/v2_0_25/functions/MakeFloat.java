package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class MakeFloat implements RealTimeTransform {

    public MakeFloat(String modelPath) {
    }

    @Override
    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);
        if (o == null) {
            return null;
        }
        return Double.class.cast(o);
    }

}
