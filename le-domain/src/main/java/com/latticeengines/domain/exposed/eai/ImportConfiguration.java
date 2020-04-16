package com.latticeengines.domain.exposed.eai;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
        @JsonSubTypes.Type(value = VdbToHdfsConfiguration.class, name = "VdbToHdfsConfiguration"),
        @JsonSubTypes.Type(value = CSVToHdfsConfiguration.class, name = "CSVToHdfsConfiguration"),
        @JsonSubTypes.Type(value = DeleteFileToHdfsConfiguration.class, name = "DeleteFileToHdfsConfiguration") })
public class ImportConfiguration extends EaiJobConfiguration {

    private List<SourceImportConfiguration> sourceConfigurations = new ArrayList<>();

    private BusinessEntity businessEntity;

    @JsonProperty("business_entity")
    public BusinessEntity getBusinessEntity() {
        return businessEntity;
    }

    @JsonProperty("business_entity")
    public void setBusinessEntity(BusinessEntity businessEntity) {
        this.businessEntity = businessEntity;
    }

    @JsonProperty("sources")
    public List<SourceImportConfiguration> getSourceConfigurations() {
        return sourceConfigurations;
    }

    @JsonProperty("sources")
    public void setSourceConfigurations(List<SourceImportConfiguration> sourceConfigurations) {
        this.sourceConfigurations = sourceConfigurations;
    }

    @JsonProperty("default_column_map")
    private Map<String, String> defaultColumnMap;

    @JsonIgnore
    public void addSourceConfiguration(SourceImportConfiguration sourceConfiguration) {
        sourceConfigurations.add(sourceConfiguration);
    }

    public Map<String, String> getDefaultColumnMap() {
        return defaultColumnMap;
    }

    public void setDefaultColumnMap(Map<String, String> defaultColumnMap) {
        this.defaultColumnMap = defaultColumnMap;
    }

    @JsonIgnore
    public void addDefaultColumn(String columnName, String value) {
        if (defaultColumnMap == null) {
            defaultColumnMap = new HashMap<>();
        }
        defaultColumnMap.put(columnName, value);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
