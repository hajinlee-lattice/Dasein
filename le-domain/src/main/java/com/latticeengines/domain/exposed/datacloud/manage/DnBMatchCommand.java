package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;
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
import javax.persistence.Table;

import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "DnBMatchCommand")
@JsonIgnoreProperties(ignoreUnknown = true)
public class DnBMatchCommand implements HasPid, Serializable {

    private static final long serialVersionUID = 7038102996243668301L;

    @Id
    @JsonIgnore
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("RootOperationUID")
    @Column(name = "RootOperationUID", nullable = false, length = 100)
    private String rootOperationUid;

    @JsonProperty("BatchID")
    @Index(name = "IX_BID")
    @Column(name = "BatchID", unique = true, nullable = false, length = 20)
    private String batchId;

    @JsonProperty("RetryForBatchID")
    @Column(name = "RetryForBatchID", length = 20)
    private String retryForBatchId;

    @JsonProperty("DnBCode")
    @Enumerated(EnumType.STRING)
    @Column(name = "DnBCode", nullable = false ,length = 20)
    private DnBReturnCode dnbCode;

    @JsonProperty("Message")
    @Column(name = "Message", nullable = false, length = 255)
    private String message;

    @JsonProperty("Size")
    @Column(name = "Size", nullable = false)
    private int size;

    @JsonProperty("AcceptedRecords")
    @Column(name = "AcceptedRecords")
    private Integer acceptedRecords;

    @JsonProperty("DiscardedRecords")
    @Column(name = "DiscardedRecords")
    private Integer discardedRecords;

    @JsonProperty("UnmatchedRecords")
    @Column(name = "UnmatchedRecords")
    private Integer unmatchedRecords;

    @JsonProperty("StartTime")
    @Column(name = "StartTime")
    private Date startTime;

    @JsonProperty("FinishTime")
    @Column(name = "FinishTime")
    private Date finishTime;

    @JsonProperty("Duration")
    @Column(name = "Duration")
    private int duration;

    public void copyContextData(DnBBatchMatchContext context) {
        // Context data to copy when DnB batch request is successfully submitted
        if (context.getDnbCode() == DnBReturnCode.SUBMITTED) {
            rootOperationUid = context.getRootOperationUid();
            batchId = context.getServiceBatchId();
            retryForBatchId = context.getRetryForServiceBatchId();
            dnbCode = context.getDnbCode();
            message = context.getDnbCode().getMessage();
            size = context.getContexts().size();
            startTime = context.getTimestamp();
        }
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getRetryForBatchId() {
        return retryForBatchId;
    }

    public void setRetryForBatchId(String retryForBatchId) {
        this.retryForBatchId = retryForBatchId;
    }

    public DnBReturnCode getDnbCode() {
        return dnbCode;
    }

    public void setDnbCode(DnBReturnCode dnbCode) {
        this.dnbCode = dnbCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getAcceptedRecords() {
        return acceptedRecords;
    }

    public void setAcceptedRecords(int acceptedRecords) {
        this.acceptedRecords = acceptedRecords;
    }

    public int getDiscardedRecords() {
        return discardedRecords;
    }

    public void setDiscardedRecords(int discardedRecords) {
        this.discardedRecords = discardedRecords;
    }

    public int getUnmatchedRecords() {
        return unmatchedRecords;
    }

    public void setUnmatchedRecords(int unmatchedRecords) {
        this.unmatchedRecords = unmatchedRecords;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(Date finishTime) {
        this.finishTime = finishTime;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

}
