package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class EnrichWebVisitJobConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;
    public static final String NAME = "enrichWebVisitJob";

    // null if no imports
    @JsonProperty
    public Integer matchedWebVisitInputIdx;

    // null if no master store for tenant (SSVI on, CDL off)
    //or all cdl tenant, cal tenant always using matchedWebVisitTable
    @JsonProperty
    public Integer masterInputIdx;

    @JsonProperty
    public Integer catalogInputIdx;

    @JsonProperty
    public Map<String,String> selectedAttributes;

    @JsonProperty
    public Integer latticeAccountTableIdx;

    @Override
    public int getNumTargets() {
        return 1;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
