package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DantePreviewResources {

    @JsonProperty("danteUrl")
    private String danteUrl;

    @JsonProperty("serverUrl")
    private String serverUrl;

    @JsonProperty("oAuthToken")
    private String oAuthToken;

    public DantePreviewResources() {
    }

    public DantePreviewResources(String danteUrl, String serverUrl, String oAuthToken) {
        this.danteUrl = danteUrl;
        this.serverUrl = serverUrl;
        this.oAuthToken = oAuthToken;
    }

    public String getDanteUrl() {
        return danteUrl;
    }

    public void setDanteUrl(String danteUrl) {
        this.danteUrl = danteUrl;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getoAuthToken() {
        return oAuthToken;
    }

    public void setoAuthToken(String oAuthToken) {
        this.oAuthToken = oAuthToken;
    }
}
