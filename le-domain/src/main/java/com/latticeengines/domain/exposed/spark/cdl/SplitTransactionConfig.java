package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class SplitTransactionConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;

    public static final String NAME = "splitTransaction";

    @JsonProperty("retainProductType")
    public List<String> retainProductType = new ArrayList<>(); // remain empty to retain both spending and analytic

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return CollectionUtils.isEmpty(retainProductType) ? 2 : retainProductType.size();
    }
}
