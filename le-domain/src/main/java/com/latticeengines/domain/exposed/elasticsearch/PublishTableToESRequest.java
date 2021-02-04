package com.latticeengines.domain.exposed.elasticsearch;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ElasticSearchExportConfig;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PublishTableToESRequest {


    @JsonProperty("ExportConfigs")
    private List<ElasticSearchExportConfig> exportConfigs;

    @JsonProperty("EsConfigs")
    private ElasticSearchConfig esConfig;

    // optional parameter, if specified, will use it instead of the
    // config in ElasticSearchExportConfig
    @JsonProperty("Signature")
    private String signature;

    // optional parameter, only use when account lookup role exists
    @JsonProperty("LookupIds")
    private List<String> lookupIds;


    public List<ElasticSearchExportConfig> getExportConfigs() {
        return exportConfigs;
    }

    public void setExportConfigs(List<ElasticSearchExportConfig> exportConfigs) {
        this.exportConfigs = exportConfigs;
    }

    public ElasticSearchConfig getEsConfig() {
        return esConfig;
    }

    public void setEsConfig(ElasticSearchConfig esConfigs) {
        this.esConfig = esConfigs;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public List<String> getLookupIds() {
        return lookupIds;
    }

    public void setLookupIds(List<String> lookupIds) {
        this.lookupIds = lookupIds;
    }
}
