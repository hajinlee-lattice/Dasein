package com.latticeengines.domain.exposed.camille.lifecycle;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BaseProperties {
    
    public BaseProperties(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
        
    }
    
    public BaseProperties() {
    }

    @JsonProperty("DisplayName")
    public String displayName;
    
    @JsonProperty("Description")
    public String description;

}
