package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "PurgeStrategy")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PurgeStrategy implements HasPid, Serializable {

    private static final long serialVersionUID = -5754360760137954518L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "Source", unique = true, nullable = false, length = 100)
    private String source;

    @Enumerated(EnumType.STRING)
    @Column(name = "SourceType", nullable = false, length = 100)
    private SourceType sourceType;

    @Column(name = "HdfsVersions")
    private Integer hdfsVersions;

    @Column(name = "HdfsDays")
    private Integer hdfsDays;

    @Column(name = "S3Days")
    private Integer s3Days;

    @Column(name = "GlacierDays")
    private Integer glacierDays;

    @Column(name = "NoBak", nullable = false)
    private boolean noBak;

    @Override
    @JsonProperty("PID")
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonProperty("PID")
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("Source")
    public String getSource() {
        return source;
    }

    @JsonProperty("Source")
    public void setSource(String source) {
        this.source = source;
    }

    @JsonProperty("SourceType")
    public SourceType getSourceType() {
        return sourceType;
    }

    @JsonProperty("SourceType")
    public void setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
    }

    @JsonProperty("HdfsVersions")
    public Integer getHdfsVersions() {
        return hdfsVersions;
    }

    @JsonProperty("HdfsVersions")
    public void setHdfsVersions(Integer hdfsVersions) {
        this.hdfsVersions = hdfsVersions;
    }

    @JsonProperty("HdfsDays")
    public Integer getHdfsDays() {
        return hdfsDays;
    }

    @JsonProperty("HdfsDays")
    public void setHdfsDays(Integer hdfsDays) {
        this.hdfsDays = hdfsDays;
    }

    @JsonProperty("S3Days")
    public Integer getS3Days() {
        return s3Days;
    }

    @JsonProperty("S3Days")
    public void setS3Days(Integer s3Days) {
        this.s3Days = s3Days;
    }

    @JsonProperty("GlacierDays")
    public Integer getGlacierDays() {
        return glacierDays;
    }

    @JsonProperty("GlacierDays")
    public void setGlacierDays(Integer glacierDays) {
        this.glacierDays = glacierDays;
    }

    @JsonProperty("NoBak")
    public boolean isNoBak() {
        return noBak;
    }

    @JsonProperty("NoBak")
    public void setNoBak(boolean noBak) {
        this.noBak = noBak;
    }

    public enum SourceType {
        INGESTION_SOURCE, GENERAL_SOURCE, TEMP_SOURCE, ACCOUNT_MASTER, ACCOUNT_MASTER_LOOKUP
    }
}
