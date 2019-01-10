package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Table(name = "DATA_INTEG_STATUS_MESSAGE")
@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataIntegrationStatusMessage implements HasPid {

    @Id
    @JsonProperty("pid")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_WORKFLOW_REQ_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private DataIntegrationStatusMonitor statusMonitor;

    @JsonProperty("workflowRequestId")
    @Column(name = "WORKFLOW_REQ_ID", nullable = true)
    private String workflowRequestId;

    @JsonProperty("messageType")
    @Column(name = "MESSAGE_TYPE", nullable = true)
    @Enumerated(EnumType.STRING)
    private MessageType messageType;

    @JsonProperty("eventType")
    @Column(name = "EVENT_TYPE", nullable = true)
    private String eventType;

    @JsonProperty("eventTime")
    @Column(name = "EVENT_TIME", nullable = true)
    @Temporal(TemporalType.TIMESTAMP)
    private Date eventTime;

    @JsonProperty("message")
    @Column(name = "MESSAGE", nullable = true)
    private String message;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public DataIntegrationStatusMonitor getStatusMonitor() {
        return statusMonitor;
    }

    public void setStatusMonitor(DataIntegrationStatusMonitor statusMonitor) {
        this.statusMonitor = statusMonitor;
        if (this.statusMonitor != null) {
            this.setWorkflowRequestId(this.statusMonitor.getWorkflowRequestId());
        }
    }

    public String getWorkflowRequestId() {
        return workflowRequestId;
    }

    public void setWorkflowRequestId(String workflowRequestId) {
        this.workflowRequestId = workflowRequestId;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
