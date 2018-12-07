package com.latticeengines.domain.exposed.spark;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class LivySession {

    public static final String STATE_IDLE = "idle";
    public static final String STATE_DEAD = "dead";

    public static final Set<String> TERMINAL_STATES = Sets.newHashSet(STATE_IDLE, STATE_DEAD);

    @JsonProperty("Host")
    private String host;

    @JsonProperty("SessionId")
    private Integer sessionId;

    @JsonProperty("State")
    private String state;

    @JsonProperty("AppId")
    private String appId;

    @JsonProperty("DriverLogUrl")
    private String driverLogUrl;

    @JsonProperty("SparkUiUrl")
    private String sparkUiUrl;

    // for jackson
    private LivySession() {}

    public LivySession(String host, int id) {
        this.host = host;
        this.sessionId = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getSessionId() {
        return sessionId;
    }

    public void setSessionId(Integer sessionId) {
        this.sessionId = sessionId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getDriverLogUrl() {
        return driverLogUrl;
    }

    public void setDriverLogUrl(String driverLogUrl) {
        this.driverLogUrl = driverLogUrl;
    }

    public String getSparkUiUrl() {
        return sparkUiUrl;
    }

    public void setSparkUiUrl(String sparkUiUrl) {
        this.sparkUiUrl = sparkUiUrl;
    }

    @JsonIgnore
    public String getSessionUrl() {
        return String.format("%s/sessions/%d", getHost(), getSessionId());
    }
}
