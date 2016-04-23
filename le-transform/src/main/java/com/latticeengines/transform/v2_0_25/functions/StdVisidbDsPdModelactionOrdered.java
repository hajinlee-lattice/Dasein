package com.latticeengines.transform.v2_0_25.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsPdModelactionOrdered implements RealTimeTransform {

    public StdVisidbDsPdModelactionOrdered(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        String modelAction = column == null ? null : String.valueOf(record.get(column));

        if(modelAction.equals("null"))
            return null;

        return calculateStdVisidbDsPdModelactionOrdered(modelAction);
    }

    public static Integer calculateStdVisidbDsPdModelactionOrdered(
            String modelAction) {
        if (StringUtils.isEmpty(modelAction))
            return null;

        modelAction = modelAction.trim().toLowerCase();

        HashMap<String, Integer> valueMap = new HashMap<String, Integer>();
        valueMap.put("low risk", 1);
        valueMap.put("low-medium risk", 2);
        valueMap.put("medium risk", 3);
        valueMap.put("medium-high risk", 4);
        valueMap.put("high risk", 5);
        valueMap.put("recent bankruptcy on file", 6);

        if (valueMap.containsKey(modelAction))
            return valueMap.get(modelAction);

        return 0;
    }
}
