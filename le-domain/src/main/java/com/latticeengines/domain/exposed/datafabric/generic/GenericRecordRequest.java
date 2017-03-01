package com.latticeengines.domain.exposed.datafabric.generic;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GenericRecordRequest {

    public static String ID_KEY = "id";
    public static String REQUEST_KEY = "recordRequest";

    @JsonProperty("Id")
    private String id;

    @JsonProperty("CustomerSpace")
    private String customerSpace;

    @JsonProperty("Version")
    private String version;

    @JsonProperty("TimeStamp")
    private long timeStamp;

    @JsonProperty("Scores")
    private List<String> stores;

    @JsonProperty("Repositories")
    private List<String> repositories;

    @JsonProperty("BatchId")
    private String batchId;

    @JsonProperty("RecordType")
    private String recordType;

    public String getId() {
        return id;
    }

    public GenericRecordRequest setId(String id) {
        this.id = id;
        return this;
    }

    public String getCustomerSpace() {
        return customerSpace;
    }

    public GenericRecordRequest setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public GenericRecordRequest setVersion(String version) {
        this.version = version;
        return this;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public GenericRecordRequest setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
        return this;
    }

    public List<String> getStores() {
        return stores;
    }

    public GenericRecordRequest setStores(List<String> destinations) {
        this.stores = destinations;
        return this;
    }

    public List<String> getRepositories() {
        return repositories;
    }

    public GenericRecordRequest setRepositories(List<String> repositories) {
        this.repositories = repositories;
        return this;
    }

    public String getBatchId() {
        return batchId;
    }

    public GenericRecordRequest setBatchId(String batchId) {
        this.batchId = batchId;
        return this;
    }

    public String getRecordType() {
        return this.recordType;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }

}
