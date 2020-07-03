package com.latticeengines.domain.exposed.datacloud.publication;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PublishToDynamoConfiguration extends PublicationConfiguration {

    // Env of Dynamo table
    @JsonProperty("Alias")
    private Alias alias;

    // Class path of data fabric entity
    @JsonProperty("EntityClass")
    private String entityClass;

    @JsonProperty("RecordType")
    private String recordType;

    @JsonProperty("AwsAccessKeyEncrypted")
    private String awsAccessKeyEncrypted;

    @JsonProperty("AwsSecretKeyEncrypted")
    private String awsSecretKeyEncrypted;

    @JsonProperty("RuntimeReadCapacity")
    private long runtimeReadCapacity;

    @JsonProperty("RuntimeWriteCapacity")
    private long runtimeWriteCapacity;

    @JsonProperty("LoadingReadCapacity")
    private long loadingReadCapacity;

    @JsonProperty("LoadingWriteCapacity")
    private long loadingWriteCapacity;

    @JsonProperty("AwsRegion")
    private String awsRegion;

    @JsonProperty("CustomerCMK")
    private String customerCMK;

    @Override
    @JsonProperty("ConfigurationType")
    public String getConfigurationType() {
        return this.getClass().getSimpleName();
    }

    public Alias getAlias() {
        return alias;
    }

    public void setAlias(Alias alias) {
        this.alias = alias;
    }

    public String getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(String entityClass) {
        this.entityClass = entityClass;
    }

    public String getRecordType() {
        return recordType;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }

    public String getAwsAccessKeyEncrypted() {
        return awsAccessKeyEncrypted;
    }

    public void setAwsAccessKeyEncrypted(String awsAccessKeyEncrypted) {
        this.awsAccessKeyEncrypted = awsAccessKeyEncrypted;
    }

    public String getAwsSecretKeyEncrypted() {
        return awsSecretKeyEncrypted;
    }

    public void setAwsSecretKeyEncrypted(String awsSecretKeyEncrypted) {
        this.awsSecretKeyEncrypted = awsSecretKeyEncrypted;
    }

    public long getRuntimeReadCapacity() {
        return runtimeReadCapacity;
    }

    public void setRuntimeReadCapacity(long runtimeReadCapacity) {
        this.runtimeReadCapacity = runtimeReadCapacity;
    }

    public long getRuntimeWriteCapacity() {
        return runtimeWriteCapacity;
    }

    public void setRuntimeWriteCapacity(long runtimeWriteCapacity) {
        this.runtimeWriteCapacity = runtimeWriteCapacity;
    }

    public long getLoadingReadCapacity() {
        return loadingReadCapacity;
    }

    public void setLoadingReadCapacity(long loadingReadCapacity) {
        this.loadingReadCapacity = loadingReadCapacity;
    }

    public long getLoadingWriteCapacity() {
        return loadingWriteCapacity;
    }

    public void setLoadingWriteCapacity(long loadingWriteCapacity) {
        this.loadingWriteCapacity = loadingWriteCapacity;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public void setAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
    }

    public String getCustomerCMK() {
        return customerCMK;
    }

    public void setCustomerCMK(String customerCMK) {
        this.customerCMK = customerCMK;
    }

    public enum Alias {
        QA, Production
    }

}
