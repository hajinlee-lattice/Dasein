package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
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
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Table(name = "DATA_INTEG_STATUS_MESSAGE")
@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataIntegrationStatusMessage implements HasPid, HasTenant, HasAuditingFields {

    @Id
    @JsonProperty("pid")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_DATA_INTEG_MONITORING_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private DataIntegrationStatusMonitor statusMonitor;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private Tenant tenant;

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

    @Column(name = "EVENT_DETAIL", columnDefinition = "'JSON'", nullable = true)
    @Type(type = "json")
    @JsonProperty("eventDetail")
    private EventDetail eventDetail;

    @JsonProperty("message")
    @Column(name = "MESSAGE", nullable = true)
    private String message;

    @JsonProperty("createdDate")
    @Column(name = "CREATED_DATE", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date createdDate;

    @JsonProperty("updatedDate")
    @Column(name = "UPDATED_DATE", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updatedDate;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
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

    @Override
    public Tenant getTenant() {
        return this.tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
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

    public EventDetail getEventDetail() {
        return eventDetail;
    }

    public void setEventDetail(EventDetail eventDetail) {
        this.eventDetail = eventDetail;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public Date getCreated() {
        return createdDate;
    }

    @Override
    public void setCreated(Date created) {
        this.createdDate = created;
    }

    @Override
    public Date getUpdated() {
        return updatedDate;
    }

    @Override
    public void setUpdated(Date updated) {
        this.updatedDate = updated;
    }
}
