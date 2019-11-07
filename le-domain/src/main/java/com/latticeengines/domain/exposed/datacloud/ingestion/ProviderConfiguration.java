package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Keep doc
 * https://confluence.lattice-engines.com/display/ENG/DataCloud+Engine+Architecture#DataCloudEngineArchitecture-Ingestion
 * up to date if there is any new change
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ClassName")
@JsonSubTypes({ @JsonSubTypes.Type(value = SftpConfiguration.class, name = "SftpConfiguration"),
        @JsonSubTypes.Type(value = SqlToTextConfiguration.class, name = "SqlToTextConfiguration"),
        @JsonSubTypes.Type(value = ApiConfiguration.class, name = "ApiConfiguration"),
        @JsonSubTypes.Type(value = SqlToSourceConfiguration.class, name = "SqlToSourceConfiguration"),
        @JsonSubTypes.Type(value = S3Configuration.class, name = "S3Configuration"),
        @JsonSubTypes.Type(value = BWRawConfiguration.class, name = "BWRawConfiguration"),
        @JsonSubTypes.Type(value = PatchBookConfiguration.class, name = "PatchBookConfiguration"), })
public abstract class ProviderConfiguration {

    // Names of subclass extending ProviderConfiguration
    private String className;

    // For single version of ingestion which has multiple files, number of files
    // to download concurrently
    protected Integer concurrentNum;

    // When looking for missing files to ingest, number of target versions to
    // backtrack
    protected Integer checkVersion;

    // If a version is finished/failed, whether to send email for notification
    protected boolean emailEnabled;

    // Only effective when EmailEnabled is true. Email to send notification for
    // ingestion status
    protected String notifyEmail;


    public ProviderConfiguration() {
        setClassName(getClass().getSimpleName());
    }

    @JsonProperty("ClassName")
    private String getClassName() {
        return className;
    }

    @JsonProperty("ClassName")
    private void setClassName(String className) {
        this.className = className;
    }

    @JsonProperty("ConcurrentNum")
    public Integer getConcurrentNum() {
        return concurrentNum;
    }

    @JsonProperty("ConcurrentNum")
    public void setConcurrentNum(Integer concurrentNum) {
        this.concurrentNum = concurrentNum;
    }

    @JsonProperty("CheckVersion")
    public Integer getCheckVersion() {
        return checkVersion;
    }

    @JsonProperty("CheckVersion")
    public void setCheckVersion(Integer checkVersion) {
        this.checkVersion = checkVersion;
    }

    @JsonProperty("EmailEnabled")
    public boolean isEmailEnabled() {
        return emailEnabled;
    }

    @JsonProperty("EmailEnabled")
    public void setEmailEnabled(boolean emailEnabled) {
        this.emailEnabled = emailEnabled;
    }

    @JsonProperty("NotifyEmail")
    public String getNotifyEmail() {
        return notifyEmail;
    }

    @JsonProperty("NotifyEmail")
    public void setNotifyEmail(String notifyEmail) {
        this.notifyEmail = notifyEmail;
    }

}
