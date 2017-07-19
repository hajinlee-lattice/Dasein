package com.latticeengines.domain.exposed.datacloud.manage;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "PipelineTransformationReport", //
uniqueConstraints = { @UniqueConstraint(columnNames = { "Pipeline", "Version", "StepName" }) })
@JsonInclude(JsonInclude.Include.NON_NULL)

public class PipelineTransformationReportByStep implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", nullable = false)
    @JsonProperty("PID")
    private Long pid;

    @Column(name = "Pipeline", nullable = false, length = 128)
    @JsonProperty("Pipeline")
    private String pipeline;

    @Column(name = "Version", nullable = false, length = 64)
    @JsonProperty("Version")
    private String version;

    @Column(name = "StepName", nullable = false)
    @JsonProperty("StepName")
    private String stepName;

    @Column(name = "TempTarget")
    @JsonProperty("TempTarget")
    private boolean tempTarget;

    @Column(name = "Transformer")
    @JsonProperty("Transformer")
    private String transformer;

    @JsonProperty("BaseSources")
    @Column(name = "BaseSources", length = 1000)
    private String baseSources;

    @JsonProperty("BaseVersions")
    @Column(name = "BaseVersions", length = 1000)
    private String baseVersions;

    @Column(name = "Executed")
    @JsonProperty("Executed")
    private boolean executed;

    @Column(name = "TargetSource")
    @JsonProperty("TargetSource")
    private String targetSource;

    @Column(name = "TargetVersion")
    @JsonProperty("TargetVersion")
    private String targetVersion;

    @Column(name = "TargetRecords")
    @JsonProperty("TargetRecords")
    private long targetRecords;

    @JsonProperty("ElapsedTime")
    @Column(name = "ElapsedTime")
    private long elapsedTime;

    @JsonProperty("HdfsPod")
    @Column(name = "HdfsPod")
    private String hdfsPod;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getPipeline() {
        return pipeline;
    }

    public void setPipeline(String pipeline) {
        this.pipeline = pipeline;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getStepName() {
        return stepName;
    }

    public void setStepName(String stepName) {
        this.stepName = stepName;
    }

    public boolean getTempTarget() {
        return tempTarget;
    }

    public void setTempTarget(boolean tempTarget) {
        this.tempTarget = tempTarget;
    }

    public String getTransformer() {
        return transformer;
    }

    public void setTransformer(String transformer) {
        this.transformer = transformer;
    }

    public String getBaseSources() {
        return baseSources;
    }

    public void setBaseSources(String baseSources) {
        this.baseSources = baseSources;
    }

    public String getBaseVersions() {
        return baseVersions;
    }

    public void setBaseVersions(String baseVersions) {
        this.baseVersions = baseVersions;
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

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public String getHdfsPod() {
        return hdfsPod;
    }

    public void setHdfsPod(String hdfsPod) {
        this.hdfsPod = hdfsPod;
    }

}
