package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "VboUsageReport", //
        indexes = { //
            @Index(name = "IX_QUIET_PERIOD", columnList = "QuietPeriodEnd"), //
            @Index(name = "IX_SUBMITTED_AT", columnList = "SubmittedAt") //
        }, //
        uniqueConstraints = {@UniqueConstraint(name = "UX_REPORT_ID", columnNames = { "ReportId" })} //
)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VboUsageReport implements HasPid {

    @Id
    @JsonIgnore
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("reportId")
    @Column(name = "ReportId", nullable = false, length = 100)
    private String reportId;

    @JsonProperty("s3Bucket")
    @Column(name = "S3Bucket", nullable = false, length = 50)
    private String s3Bucket;

    @JsonProperty("s3Path")
    @Column(name = "S3Path", nullable = false, length = 500)
    private String s3Path;

    @JsonProperty("type")
    @Column(name = "Type", nullable = false)
    @Enumerated(EnumType.STRING)
    private Type type;

    @JsonProperty("numRecords")
    @Column(name = "NumRecords")
    private Long numRecords;

    @JsonProperty("createdAt")
    @Column(name = "CreatedAt", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date createdAt;

    @JsonProperty("quietPeriodEnd")
    @Column(name = "QuietPeriodEnd")
    @Temporal(TemporalType.TIMESTAMP)
    private Date quietPeriodEnd;

    @JsonProperty("submittedAt")
    @Column(name = "SubmittedAt")
    @Temporal(TemporalType.TIMESTAMP)
    private Date submittedAt;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getReportId() {
        return reportId;
    }

    public void setReportId(String reportId) {
        this.reportId = reportId;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public String getS3Path() {
        return s3Path;
    }

    public void setS3Path(String s3Path) {
        this.s3Path = s3Path;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Long getNumRecords() {
        return numRecords;
    }

    public void setNumRecords(Long numRecords) {
        this.numRecords = numRecords;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public Date getQuietPeriodEnd() {
        return quietPeriodEnd;
    }

    public void setQuietPeriodEnd(Date quietPeriodEnd) {
        this.quietPeriodEnd = quietPeriodEnd;
    }

    public Date getSubmittedAt() {
        return submittedAt;
    }

    public void setSubmittedAt(Date submittedAt) {
        this.submittedAt = submittedAt;
    }

    public enum Type {
        Batch, API, Monitoring
    }

}
