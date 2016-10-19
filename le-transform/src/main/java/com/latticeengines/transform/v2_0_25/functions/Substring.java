package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.transform.exposed.RealTimeTransform;

public abstract class Substring implements RealTimeTransform {

    private static final long serialVersionUID = 1L;

    public String transform(Integer index1, Integer index2, Object o) {
        if (o == null) {
            return null;
        }
        
        String value = String.valueOf(o);
        
        return value.substring(index1, index2);
    }

    protected String getValueToSubstring(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        
        Object value = record.get(column);
        
        if (value == null) {
            return null;
        }
        
        if (!(value instanceof String)) {
            return null;
        }
        
        return ((String) value).trim();
    }

}
