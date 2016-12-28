package com.latticeengines.domain.exposed.datacloud.transformation;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransformationStepReport {

    @JsonProperty("Transformer")
    private String transformer;

    @JsonProperty("BaseSources")
    private List<String> baseSources;

    @JsonProperty("BaseVersions")
    private List<String> baseVersions;

    @JsonProperty("Executed")
    private boolean executed;

    @JsonProperty("TargetSource")
    private String targetSource;

    @JsonProperty("TargetVersion")
    private String targetVersion;

    @JsonProperty("TargetRecords")
    private long targetRecords;

    public String getTransformer() {
        return transformer;
    }

    public void setTransformer(String transformer) {
        this.transformer = transformer;
    }

    public List<String> getBaseSources() {
        return baseSources;
    }

    public void setBaseSources(List<String> baseSources) {
        this.baseSources = baseSources;
    }

    public List<String> getBaseVersions() {
        return baseVersions;
    }

    public void setBaseVersions(List<String> baseVersions) {
        this.baseVersions = baseVersions;
    }

    public void addBaseSource(String sourceName, String version) {
        if (baseSources == null) {
            baseSources = new ArrayList<String>();
        }
        if (baseVersions == null) {
            baseVersions = new ArrayList<String>();
        }
        baseSources.add(sourceName);
        baseVersions.add(version);
    }

    public void setTargetSource(String sourceName, String version, long records) {
        this.targetSource = sourceName;
        this.targetVersion = version;
        this.targetRecords = records;
    }

    public String getTargetSource() {
        return targetSource;
    }

    public void setTargetSource(String targetSource) {
        this.targetSource = targetSource;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(String targetVersion) {
        this.targetVersion = targetVersion;
    }

    public long getTargetRecords() {
        return targetRecords;
    }

    public void setTargetRecords(long targetRecords) {
        this.targetRecords = targetRecords;
    }

    public boolean getExecuted() {
        return executed;
    }

    public void setExecuted(boolean executed) {
        this.executed = executed;
    }
}
