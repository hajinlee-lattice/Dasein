package com.latticeengines.domain.exposed.eai;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class ImportConfiguration implements HasName {
    private String name;
    private String customer;
    private List<SourceImportConfiguration> sourceConfigurations = new ArrayList<>();

    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("sources")
    public List<SourceImportConfiguration> getSourceConfigurations() {
        return sourceConfigurations;
    }

    @JsonProperty("sources")
    public void setSourceConfigurations(List<SourceImportConfiguration> sourceConfigurations) {
        this.sourceConfigurations = sourceConfigurations;
    }
    
    @JsonIgnore
    public void addSourceConfiguration(SourceImportConfiguration sourceConfiguration) {
        sourceConfigurations.add(sourceConfiguration);
    }
    
    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonProperty("customer")
    public String getCustomer() {
        return customer;
    }

    @JsonProperty("customer")
    public void setCustomer(String customer) {
        this.customer = customer;
    }
}
