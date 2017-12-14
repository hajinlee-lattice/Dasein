package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PurgeSource implements Serializable {
    private static final long serialVersionUID = -8722685279338905598L;

    @JsonProperty("SourceName")
    private String sourceName;

    @JsonProperty("Version")
    private String version;

    @JsonProperty("HdfsPaths")
    private List<String> hdfsPaths;

    @JsonProperty("HiveTables")
    private List<String> hiveTables;

    @JsonProperty("ToBak")
    private boolean toBak;

    public PurgeSource() {

    }

    public PurgeSource(String sourceName, String version, List<String> hdfsPaths, List<String> hiveTables,
            boolean toBak) {
        this.sourceName = sourceName;
        this.version = version;
        this.hdfsPaths = hdfsPaths;
        this.hiveTables = hiveTables;
        this.toBak = toBak;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public List<String> getHdfsPaths() {
        return hdfsPaths;
    }

    public void setHdfsPaths(List<String> hdfsPaths) {
        this.hdfsPaths = hdfsPaths;
    }

    public List<String> getHiveTables() {
        return hiveTables;
    }

    public void setHiveTables(List<String> hiveTables) {
        this.hiveTables = hiveTables;
    }

    public boolean isToBak() {
        return toBak;
    }

    public void setToBak(boolean toBak) {
        this.toBak = toBak;
    }

}
