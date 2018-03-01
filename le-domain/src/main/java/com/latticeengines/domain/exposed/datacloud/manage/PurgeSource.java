package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PurgeSource implements Serializable {
    private static final long serialVersionUID = -8722685279338905598L;

    @JsonProperty("SourceName")
    private String sourceName;

    @JsonProperty("HdfsPaths")
    private List<String> hdfsPaths;

    @JsonProperty("HiveTables")
    private List<String> hiveTables;

    @JsonProperty("ToBak")
    private boolean toBak;

    @JsonProperty("S3Days")
    private Integer s3Days;

    @JsonProperty("GlacierDays")
    private Integer glacierDays;

    public PurgeSource() {

    }

    public PurgeSource(String sourceName, List<String> hdfsPaths, List<String> hiveTables,
            boolean toBak) {
        this.sourceName = sourceName;
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

    public Integer getS3Days() {
        return s3Days;
    }

    public void setS3Days(Integer s3Days) {
        this.s3Days = s3Days;
    }

    public Integer getGlacierDays() {
        return glacierDays;
    }

    public void setGlacierDays(Integer glacierDays) {
        this.glacierDays = glacierDays;
    }

}
