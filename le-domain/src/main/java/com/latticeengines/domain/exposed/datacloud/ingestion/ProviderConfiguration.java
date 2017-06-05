package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ClassName")
@JsonSubTypes({ @JsonSubTypes.Type(value = SftpConfiguration.class, name = "SftpConfiguration"),
        @JsonSubTypes.Type(value = SqlToTextConfiguration.class, name = "SqlToTextConfiguration"),
        @JsonSubTypes.Type(value = ApiConfiguration.class, name = "ApiConfiguration"),
        @JsonSubTypes.Type(value = SqlToSourceConfiguration.class, name = "SqlToSourceConfiguration")
              })
public abstract class ProviderConfiguration {
    private String className;
    protected Integer concurrentNum;
    protected Integer checkVersion;
    protected boolean emailEnabled;
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
