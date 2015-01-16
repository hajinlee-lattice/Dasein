package com.latticeengines.dataplatform.runtime.load;

import java.util.Map;

public enum LoadProperty {
    
    EXCLUDETIMESTAMPCOLUMNS("true");
    
    private String defaultValue;
    
    LoadProperty(String defaultValue) {
        this.defaultValue = defaultValue;
    }
    
    public String getDefaultValue() {
        return defaultValue;
    }
    
    public String getValue(Map<String, String> properties) {
        String value = properties.get(name());
        
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }
    
}
