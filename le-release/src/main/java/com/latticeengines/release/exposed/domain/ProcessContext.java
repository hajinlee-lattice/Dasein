package com.latticeengines.release.exposed.domain;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;

@Component("processContext")
public class ProcessContext {

    private String releaseVersion;

    private String nextReleaseVersion;

    private String revision;

    private String product;

    private List<String> projectsShouldUploadToNexus;

    public ProcessContext() {
        projectsShouldUploadToNexus = Arrays.asList(new String[] { "le-pls", "le-propdata" });
    }

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

    public String toString() {
        return JsonUtils.serialize(this);
    }
}
