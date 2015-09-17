package com.latticeengines.propdata.eai.service;

import java.util.HashMap;
import java.util.Map;

public class PropDataContext {

    private Map<String, Object> properties = new HashMap<String, Object>();

    public void setProperty(String name, Object value) {
        properties.put(name, value);
    }

    public <T> T getProperty(String name, Class<T> valueClass) {
        Object value = properties.get(name);

        if (value == null) {
            return null;
        }

        if (valueClass.isInstance(value)) {
            return valueClass.cast(value);
        } else {
            throw new RuntimeException("Value is not of type " + valueClass);
        }
    }

    public Map<String, Object> getMap() {
        return properties;
    }
}
