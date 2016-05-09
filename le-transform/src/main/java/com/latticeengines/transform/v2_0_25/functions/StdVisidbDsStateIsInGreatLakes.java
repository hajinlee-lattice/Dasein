package com.latticeengines.transform.v2_0_25.functions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsStateIsInGreatLakes implements RealTimeTransform {

    static HashSet<String> valueMap = new HashSet<String>(Arrays.asList("OH", "MI", "IL", "WI", "IN"));

    public StdVisidbDsStateIsInGreatLakes(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return false;

        String s = n.toString().trim().toUpperCase();

        return getDSStateIsInGreatLakes(s);
    }

    public static Boolean getDSStateIsInGreatLakes(String state) {
        if (StringUtils.isEmpty(state))
            return false;

        return valueMap.contains(state);
    }
}
