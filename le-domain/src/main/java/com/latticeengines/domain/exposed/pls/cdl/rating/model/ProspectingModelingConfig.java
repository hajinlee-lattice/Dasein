package com.latticeengines.domain.exposed.pls.cdl.rating.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProspectingModelingConfig implements AdvancedModelingConfig {

    @Override
    public void copyConfig(AdvancedModelingConfig config) {
        // TODO Auto-generated method stub
        
    }

}
