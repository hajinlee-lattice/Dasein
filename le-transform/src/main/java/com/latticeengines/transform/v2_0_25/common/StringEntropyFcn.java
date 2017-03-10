package com.latticeengines.transform.v2_0_25.common;

import java.util.HashMap;
import java.util.Map;

public class StringEntropyFcn extends TransformWithImputationFunctionBase {

    private int maxStringLen;

    public StringEntropyFcn(Object imputation, int maxStringLen) {
        super(imputation);
        this.maxStringLen = maxStringLen;
    }

    @Override
    public double execute(String s) {
        if (s == null) {
            return getImputation();
        }

        Map<Character, Integer> occurences = new HashMap<Character, Integer>();

        for (int i = 0; i < s.length(); i++) {
            if (occurences.containsKey(s.charAt(i))) {
                int oldValue = occurences.get(s.charAt(i));
                occurences.put(s.charAt(i), oldValue + 1);
            } else {
                occurences.put(s.charAt(i), 1);
            }
        }

        double n = 0.0;
        final double length = s.length();
        for (Map.Entry<Character, Integer> entry : occurences.entrySet()) {
            final double prob = entry.getValue().doubleValue() / length;
            n -= prob * Math.log(prob);
        }
        final double entropy = n / (Math.min(length, maxStringLen) + 0.00001);
        return entropy;
    }

}
