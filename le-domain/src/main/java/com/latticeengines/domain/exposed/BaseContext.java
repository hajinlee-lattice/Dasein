package com.latticeengines.domain.exposed;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.yarn.YarnProperty;

public class BaseContext {

    private Map<String, Object> properties = new HashMap<String, Object>();

    public BaseContext() {
    }

    public BaseContext(Configuration yarnConfiguration) {
        setProperty(YarnProperty.HADOOPCONFIG, yarnConfiguration);
    }

    public BaseContext(BaseContext other) {
        this.properties = new HashMap<>();
        this.properties.putAll(other.properties);
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

    public <T> T getRequiredProperty(String propertyName, Class<T> propertyValueClass) {
        T value = getProperty(propertyName, propertyValueClass);
        if (value == null) {
            throw new RuntimeException(String
                    .format("Required property %s is not specified in context", propertyName));
        }
        return value;
    }

    public <T> T getProperty(String propertyName, Class<T> propertyValueClass, T dflt) {
        T value = getProperty(propertyName, propertyValueClass);
        if (value == null) {
            return dflt;
        }
        return value;
    }

    public Set<Map.Entry<String, Object>> getEntries() {
        return properties.entrySet();
    }

    public boolean containsProperty(String propertyName) {
        return properties.containsKey(propertyName);
    }

}
