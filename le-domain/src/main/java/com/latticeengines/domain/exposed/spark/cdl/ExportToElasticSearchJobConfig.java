package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ExportToElasticSearchJobConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;
    public static final String NAME = "exportToElasticSearchJob";

    @Override
    public String getName() {
        return NAME;
    }
    //es connect configuration
    @JsonProperty("es_config")
    public ElasticSearchConfig esConfig;
    //tableRoleInCollection -> tableInputFinalId
    @JsonProperty("input_idx")
    public Map<String, List<Integer>> inputIdx = new HashMap<>();
    @JsonProperty("lookup_idx")
    public Integer lookupIdx;
    @JsonProperty("customer_space")
    public String customerSpace;
    //BusinessEntity -> ESVersion
    @JsonProperty("entityWithESVersionMap")
    public Map<String, String> entityWithESVersionMap = new HashMap<>();

    @Override
    public int getNumTargets() {
        return 0;
    }
}
