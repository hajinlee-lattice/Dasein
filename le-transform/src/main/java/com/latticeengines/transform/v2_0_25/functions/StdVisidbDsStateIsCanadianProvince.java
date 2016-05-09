package com.latticeengines.transform.v2_0_25.functions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsStateIsCanadianProvince implements RealTimeTransform {

    static HashSet<String> valueMap = new HashSet<String>(
            Arrays.asList("ON", "AB", "NL", "MB", "NB", "BC", "YT", "SK", "QC", "PE", "NS", "NT", "NU"));

    public StdVisidbDsStateIsCanadianProvince(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return false;

        String s = n.toString().trim().toUpperCase();

        return getDSStateIsCanadianProvince(s);
    }

    public static Boolean getDSStateIsCanadianProvince(String state) {
        if (StringUtils.isEmpty(state))
            return false;

        return valueMap.contains(state);
    }
}
