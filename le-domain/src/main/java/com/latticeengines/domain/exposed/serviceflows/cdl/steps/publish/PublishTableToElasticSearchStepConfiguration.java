package com.latticeengines.domain.exposed.serviceflows.cdl.steps.publish;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ElasticSearchExportConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;

public class PublishTableToElasticSearchStepConfiguration extends SparkJobStepConfiguration {


    @JsonProperty("export_configs")
    private List<ElasticSearchExportConfig> exportConfigs;

    @JsonProperty("es_configs")
    private ElasticSearchConfig esConfigs;

    @JsonProperty("signature")
    private String signature;

    public List<ElasticSearchExportConfig> getExportConfigs() {
        return exportConfigs;
    }

    public void setExportConfigs(List<ElasticSearchExportConfig> exportConfigs) {
        this.exportConfigs = exportConfigs;
    }

    public ElasticSearchConfig getEsConfigs() {
        return esConfigs;
    }

    public void setEsConfigs(ElasticSearchConfig esConfigs) {
        this.esConfigs = esConfigs;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }
}
