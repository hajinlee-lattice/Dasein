package com.latticeengines.common.exposed.visitor;

import java.util.HashMap;
import java.util.Map;

public class VisitorContext {

    private final Map<String, Object> map = new HashMap<String, Object>();

    public VisitorContext () {
    }

    public Object getProperty(String key) {
        return map.get(key);
    }

    public void setProperty(String key, Object value) {
        map.put(key, value);
    }
}
