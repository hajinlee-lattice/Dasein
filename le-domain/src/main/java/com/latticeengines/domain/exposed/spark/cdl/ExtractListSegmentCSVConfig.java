package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ExtractListSegmentCSVConfig extends SparkJobConfig {

    private static final long serialVersionUID = -5779498867998360393L;

    public static final String NAME = "extractListSegmentCSV";

    @JsonProperty("targetNums")
    private int targetNums;

    @JsonProperty("accountAttributes")
    private List<String> accountAttributes;

    @JsonProperty("contactAttributes")
    private List<String> contactAttributes;

    @JsonProperty("csvAdaptor")
    private CSVAdaptor csvAdaptor;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return targetNums;
    }

    public void setTargetNums(int targetNums) {
        this.targetNums = targetNums;
    }

    public List<String> getAccountAttributes() {
        return accountAttributes;
    }

    public void setAccountAttributes(List<String> accountAttributes) {
        this.accountAttributes = accountAttributes;
    }

    public List<String> getContactAttributes() {
        return contactAttributes;
    }

    public void setContactAttributes(List<String> contactAttributes) {
        this.contactAttributes = contactAttributes;
    }

    public CSVAdaptor getCsvAdaptor() {
        return csvAdaptor;
    }

    public void setCsvAdaptor(CSVAdaptor csvAdaptor) {
        this.csvAdaptor = csvAdaptor;
    }
}
