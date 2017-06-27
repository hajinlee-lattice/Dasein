package com.latticeengines.domain.exposed.dante;

public class DantePreviewResources {
    private String url;
    private String oAuthToken;

    public DantePreviewResources() {
    }

    public DantePreviewResources(String url, String oAuthToken) {
        this.url = url;
        this.oAuthToken = oAuthToken;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getoAuthToken() {
        return oAuthToken;
    }

    public void setoAuthToken(String oAuthToken) {
        this.oAuthToken = oAuthToken;
    }
}
