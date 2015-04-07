package com.latticeengines.domain.exposed.camille.lifecycle;


public class BaseProperties {
    
    public BaseProperties(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
        
    }
    
    public BaseProperties() {
    }

    public String displayName;
    
    public String description;

}
