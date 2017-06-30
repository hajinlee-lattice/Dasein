package com.latticeengines.domain.exposed.dante;

public class DantePreviewResources {
    private String danteUrl;
    private String serverUrl;

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

    private String oAuthToken;

    public DantePreviewResources() {
    }

    public DantePreviewResources(String danteUrl, String serverUrl, String oAuthToken) {
        this.danteUrl = danteUrl;
        this.serverUrl = serverUrl;
        this.oAuthToken = oAuthToken;
    }

    public String getoAuthToken() {
        return oAuthToken;
    }

    public void setoAuthToken(String oAuthToken) {
        this.oAuthToken = oAuthToken;
    }
}
