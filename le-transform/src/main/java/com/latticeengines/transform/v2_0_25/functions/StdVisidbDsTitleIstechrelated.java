package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;
import java.util.regex.Pattern;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsTitleIstechrelated implements RealTimeTransform {

    public StdVisidbDsTitleIstechrelated(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return false;

        String s = n.toString().toLowerCase();

        return Pattern
                .matches(
                        "(.*?\\b)eng(.*)|(.*?\\b)tech(.*)|(.*?\\b)info(.*)|(.*)dev(.*)",
                        s) ? true : false;
    }
}
