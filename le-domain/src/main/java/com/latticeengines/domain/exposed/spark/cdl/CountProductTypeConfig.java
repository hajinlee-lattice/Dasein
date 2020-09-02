package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CountProductTypeConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;

    public static final String NAME = "countProductTypeConfig";

    @JsonProperty
    public List<String> types = new ArrayList<>(); // product types to count

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 0;
    }
}
