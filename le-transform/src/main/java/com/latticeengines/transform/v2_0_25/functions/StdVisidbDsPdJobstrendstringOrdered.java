package com.latticeengines.transform.v2_0_25.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsPdJobstrendstringOrdered implements RealTimeTransform {

    public StdVisidbDsPdJobstrendstringOrdered(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return null;

        String s = n.toString().toLowerCase();

        return calculateStdVisidbDsPdJobstrendstringOrdered(s);
    }

    public static Integer calculateStdVisidbDsPdJobstrendstringOrdered(
            String hiringLevel) {
        if (StringUtils.isEmpty(hiringLevel))
            return null;

        HashMap<String, Integer> valueMap = new HashMap<String, Integer>();
        valueMap.put("moderately hiring", 1);
        valueMap.put("significantly hiring", 2);
        valueMap.put("aggressively hiring", 3);

        hiringLevel = hiringLevel.trim().toLowerCase();

        if (valueMap.containsKey(hiringLevel))
            return valueMap.get(hiringLevel);

        return 0;
    }
}
