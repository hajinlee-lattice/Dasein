package com.latticeengines.release.exposed.domain;

import java.util.List;

public class ProcessContext {

    private String releaseVersion;

    private String nextReleaseVersion;

    private String revision;

    private String product;

    private List<String> projectsShouldUploadToNexus;

    private String url;

    private String responseMessage;

    private int statusCode = -1;

    public String getReleaseVersion() {
        return this.releaseVersion;
    }

    public void setReleaseVersion(String releaseVersion) {
        this.releaseVersion = releaseVersion;
    }

    public String getNextReleaseVersion() {
        return nextReleaseVersion;
    }

    public void setNextReleaseVersion(String nextReleaseVersion) {
        this.nextReleaseVersion = nextReleaseVersion;
    }

    public String getRevision() {
        return this.revision;
    }

    public void setRevision(String revision) {
        this.revision = revision;
    }

    public String getProduct() {
        return this.product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public List<String> getProjectsShouldUploadToNexus() {
        return projectsShouldUploadToNexus;
    }

    public void setProjectsShouldUploadToNexus(List<String> projectsShouldUploadToNexus) {
        this.projectsShouldUploadToNexus = projectsShouldUploadToNexus;
    }

    public String getUrl() {
        return this.url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getResponseMessage() {
        return this.responseMessage;
    }

    public void setResponseMessage(String responseMessage) {
        this.responseMessage = responseMessage;
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }
}
