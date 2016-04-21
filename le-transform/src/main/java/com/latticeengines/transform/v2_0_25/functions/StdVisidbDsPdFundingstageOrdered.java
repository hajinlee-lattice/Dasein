package com.latticeengines.transform.v2_0_25.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsPdFundingstageOrdered implements RealTimeTransform {

    public StdVisidbDsPdFundingstageOrdered(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return null;

        String s = n.toString().toLowerCase();

        return calculateStdVisidbDsPdFundingstageOrdered(s);
    }

    public static Integer calculateStdVisidbDsPdFundingstageOrdered(
            String fundingStage) {
        if (StringUtils.isEmpty(fundingStage))
            return null;

        HashMap<String, Integer> valueMap = new HashMap<String, Integer>();
        valueMap.put("startup/seed", 1);
        valueMap.put("early stage", 2);
        valueMap.put("expansion", 3);
        valueMap.put("later stage", 4);

        fundingStage = fundingStage.trim().toLowerCase();

        if (valueMap.containsKey(fundingStage))
            return valueMap.get(fundingStage);

        return 0;
    }
}
