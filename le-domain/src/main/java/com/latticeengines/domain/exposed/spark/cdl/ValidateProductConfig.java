package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ValidateProductConfig extends SparkJobConfig implements Serializable {

    public static final String NAME = "validateProduct";

    private boolean checkProductName;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 2;
    }

    @JsonProperty("CheckProductName")
    public boolean getCheckProductName() {
        return checkProductName;
    }

    @JsonProperty("CheckProductName")
    public void setCheckProductName(boolean checkProductName) {
        this.checkProductName = checkProductName;
    }
}
