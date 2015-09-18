package com.latticeengines.domain.exposed;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

public class BaseContext {

    private Map<String, Object> properties = new HashMap<String, Object>();

    public BaseContext() {
    }
    
    public BaseContext(Configuration yarnConfiguration) {
        setProperty(BaseProperty.HADOOPCONFIG, yarnConfiguration);
    }

    public void setProperty(String propertyName, Object propertyValue) {
        properties.put(propertyName, propertyValue);
    }

    public <T> T getProperty(String propertyName, Class<T> propertyValueClass) {
        Object value = properties.get(propertyName);

        if (value == null) {
            return null;
        }

        if (propertyValueClass.isInstance(value)) {
            return propertyValueClass.cast(value);
        } else {
            throw new RuntimeException("Value is not of type " + propertyValueClass);
        }
    }

    public Set<Map.Entry<String, Object>> getEntries() {
        return properties.entrySet();
    }
    
    public boolean containsProperty(String propertyName) {
        return properties.containsKey(propertyName);
    }

}
