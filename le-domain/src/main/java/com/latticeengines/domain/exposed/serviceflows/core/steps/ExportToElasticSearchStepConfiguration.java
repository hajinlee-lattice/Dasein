package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;

public class ExportToElasticSearchStepConfiguration extends SparkJobStepConfiguration {

    @JsonProperty("es_config")
    private ElasticSearchConfig esConfig;

    //this tenant provide the tableRole table
    @JsonProperty("original_tenant")
    private String originalTenant;

    public ElasticSearchConfig getEsConfig() {
        return esConfig;
    }

    public void setEsConfig(ElasticSearchConfig esConfig) {
        this.esConfig = esConfig;
    }

    public String getOriginalTenant() {
        return originalTenant;
    }

    public void setOriginalTenant(String originalTenant) {
        this.originalTenant = originalTenant;
    }
}
