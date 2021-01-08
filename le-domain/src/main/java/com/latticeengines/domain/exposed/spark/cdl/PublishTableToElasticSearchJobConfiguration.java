package com.latticeengines.domain.exposed.spark.cdl;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class PublishTableToElasticSearchJobConfiguration extends SparkJobConfig {
    public static final String NAME = "PublishTableToElasticSearchJob";

    @JsonProperty("IndexToRole")
    private Map<Integer, TableRoleInCollection> indexToRole;

    @JsonProperty("IndexToSignature")
    private Map<Integer, String> indexToSignature;

    @JsonProperty("ESConfigs")
    private ElasticSearchConfig esConfigs;

    @JsonProperty("CustomerSpace")
    private String customerSpace;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 0;
    }

    public Map<Integer, TableRoleInCollection> getIndexToRole() {
        return indexToRole;
    }

    public void setIndexToRole(Map<Integer, TableRoleInCollection> indexToRole) {
        this.indexToRole = indexToRole;
    }

    public Map<Integer, String> getIndexToSignature() {
        return indexToSignature;
    }

    public void setIndexToSignature(Map<Integer, String> indexToSignature) {
        this.indexToSignature = indexToSignature;
    }

    public ElasticSearchConfig getEsConfigs() {
        return esConfigs;
    }

    public void setEsConfigs(ElasticSearchConfig esConfigs) {
        this.esConfigs = esConfigs;
    }

    public String getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }
}
