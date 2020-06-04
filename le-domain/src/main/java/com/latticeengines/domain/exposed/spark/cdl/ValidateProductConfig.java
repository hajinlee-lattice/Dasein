package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ValidateProductConfig extends SparkJobConfig implements Serializable {

    public static final String NAME = "validateProduct";

    // this is path number for user uploaded file, defaults to 1, for vdb, larger than 1
    private int inputPathNum = 1;

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

    @JsonProperty("InputPathNum")
    public int getInputPathNum() {
        return inputPathNum;
    }

    @JsonProperty("InputPathNum")
    public void setInputPathNum(int inputPathNum) {
        this.inputPathNum = inputPathNum;
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
