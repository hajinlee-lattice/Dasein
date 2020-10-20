package com.latticeengines.domain.exposed.spark.dcp;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class AnalyzeUsageConfig extends SparkJobConfig {

    public static final String NAME = "AnalyzeUsage";

    @JsonProperty("OutputFileds")
    private List<String> outputFields;

    @JsonProperty("RawOutputMap")
    private Map<String, String> RawOutputMap;

    @JsonProperty("DRTAttr")
    private String DRTAttr;

    @JsonProperty("UploadId")
    private String uploadId;

    @JsonProperty("SubscriberNumber")
    private String subscriberNumber;

    @JsonProperty("SubscriberName")
    private String subscriberName;

    @JsonProperty("SubscriberCountry")
    private String subscriberCountry;

    @JsonProperty("ContractStartTime")
    private String contractStartTime;

    @JsonProperty("ContractEndTime")
    private String contractEndTime;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 1;
    }

    public List<String> getOutputFields() {
        return outputFields;
    }

    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }

    public Map<String, String> getRawOutputMap() {
        return RawOutputMap;
    }

    public void setRawOutputMap(Map<String, String> rawOutputMap) {
        RawOutputMap = rawOutputMap;
    }

    public String getDRTAttr() {
        return DRTAttr;
    }

    public void setDRTAttr(String DRTAttr) {
        this.DRTAttr = DRTAttr;
    }

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public String getSubscriberNumber() {
        return subscriberNumber;
    }

    public void setSubscriberNumber(String subscriberNumber) {
        this.subscriberNumber = subscriberNumber;
    }

    public String getSubscriberName() {
        return subscriberName;
    }

    public void setSubscriberName(String subscriberName) {
        this.subscriberName = subscriberName;
    }

    public String getSubscriberCountry() {
        return subscriberCountry;
    }

    public void setSubscriberCountry(String subscriberCountry) {
        this.subscriberCountry = subscriberCountry;
    }

    public String getContractStartTime() {
        return contractStartTime;
    }

    public void setContractStartTime(String contractStartTime) {
        this.contractStartTime = contractStartTime;
    }

    public String getContractEndTime() {
        return contractEndTime;
    }

    public void setContractEndTime(String contractEndTime) {
        this.contractEndTime = contractEndTime;
    }
}
