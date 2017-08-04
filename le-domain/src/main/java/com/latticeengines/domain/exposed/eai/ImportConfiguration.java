package com.latticeengines.domain.exposed.eai;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({ @JsonSubTypes.Type(value = VdbToHdfsConfiguration.class, name = "VdbToHdfsConfiguration"),
        @JsonSubTypes.Type(value = CSVToHdfsConfiguration.class, name = "CSVToHdfsConfiguration"), })
public class ImportConfiguration extends EaiJobConfiguration {

    private List<SourceImportConfiguration> sourceConfigurations = new ArrayList<>();

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

}
