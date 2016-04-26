package com.latticeengines.transform.v2_0_25.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdEntropy implements RealTimeTransform {

    public StdEntropy(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        String n = String.valueOf(record.get(column));

        if(n.equals("null"))
            return null;

        return calculateStdEntropy(n);
    }

    static Double calculateStdEntropy(String s) {
        if (StringUtils.isEmpty(s) || "null".equals(s))
            return null;

        HashMap<Character, Integer> occurences = new HashMap<Character, Integer>();

        for (int i = 0; i < s.length(); i++) {
            if (occurences.containsKey(s.charAt(i))) {
                int oldValue = occurences.get(s.charAt(i));
                occurences.put(s.charAt(i), oldValue + 1);
            } else {
                occurences.put(s.charAt(i), 1);
            }
        }

        if (occurences.keySet().size() < 2)
            return 0.0;

        Double e = 0.0;
        int depth = s.length();
        for (Character c : occurences.keySet()) {
            float p = (occurences.get(c)) / (float) depth;
            e -= p * (Math.log(p) / (Math.log(2) + 1e-10));
        }

        return e / s.length();
    }
}
