package com.latticeengines.domain.exposed.pls.cdl.rating.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProspectingModelingConfig implements AdvancedModelingConfig {
    private String dataCloudVersion;

    @Override
    public void copyConfig(AdvancedModelingConfig config) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String getDataCloudVersion() { return dataCloudVersion; }

    public void setDataCloudVersion(String dataCloudVersion) { this.dataCloudVersion = dataCloudVersion; }

}
