package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;
import java.util.regex.Pattern;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsTitleIsacademic implements RealTimeTransform {

    public StdVisidbDsTitleIsacademic(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return false;

        String s = n.toString().toLowerCase();

        return (Pattern
                .matches(
                        "(.*?\\b)student(.*)|(.*?\\b)researcher(.*)|(.*?\\b)professor(.*)|(.*)dev(.*)|(.*?\\b)programmer(.*)",
                        s) && StdVisidbDsTitleLevel
                .calculateStdVisidbDsTitleLevel(s) == 0) ? true : false;
    }
}
