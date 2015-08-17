package com.latticeengines.domain.exposed.eai;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;

public class ImportContext {

    private Map<String, Object> properties = new HashMap<String, Object>();

    public void setProperty(String name, Object value) {
        properties.put(name, value);
    }

    public ImportContext(Configuration yarnConfiguration){
        setProperty(ImportProperty.HADOOPCONFIG, yarnConfiguration);
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

    public Set<Map.Entry<String, Object>> getEntries() {
        return properties.entrySet();
    }
}
