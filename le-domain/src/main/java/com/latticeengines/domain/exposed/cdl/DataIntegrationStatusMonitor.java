package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.ParamDef;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "DATA_INTEG_STATUS_MONITORING", indexes = {
        @Index(name = "WORKFLOW_REQ_ID", columnList = "WORKFLOW_REQ_ID") })
@FilterDef(name = "tenantFilter", defaultCondition = "FK_TENANT_ID = :tenantFilterId", parameters = {
        @ParamDef(name = "tenantFilterId", type = "java.lang.Long") })
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataIntegrationStatusMonitor
        implements HasPid, HasId<String>, HasTenant, HasAuditingFields {

    @Id
    @JsonProperty("pid")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty("operation")
    @Column(name = "OPERATION", nullable = true)
    private String operation;

    @JsonProperty("entityName")
    @Column(name = "ENTITY_NAME", nullable = true)
    private String entityName;

    @JsonProperty("entityId")
    @Column(name = "ENTITY_ID", nullable = true)
    private String entityId;

    @JsonProperty("externalSystemId")
    @Column(name = "EXTERNAL_SYSTEM_ID", nullable = true)
    private String externalSystemId;

    @JsonProperty("workflowRequestId")
    @Column(name = "WORKFLOW_REQ_ID", unique = true, nullable = false)
    private String workflowRequestId;

    @JsonProperty("sourceFile")
    @Column(name = "SOURCE_FILE", nullable = true)
    private String sourceFile;

    @JsonProperty("errorFile")
    @Column(name = "ERROR_FILE", nullable = true)
    private String errorFile;

    @JsonProperty("status")
    @Column(name = "STATUS", nullable = true)
    private String status;

    @JsonProperty("eventSubmittedTime")
    @Column(name = "EVENT_SUBMITTED_TIME", nullable = true)
    @Temporal(TemporalType.TIMESTAMP)
    private Date eventSubmittedTime;

    @JsonProperty("eventStartedTime")
    @Column(name = "EVENT_STARTED_TIME", nullable = true)
    @Temporal(TemporalType.TIMESTAMP)
    private Date eventStartedTime;

    @JsonProperty("eventCompletedTime")
    @Column(name = "EVENT_COMPLETED_TIME", nullable = true)
    @Temporal(TemporalType.TIMESTAMP)
    private Date eventCompletedTime;

    @JsonProperty("createdDate")
    @Column(name = "CREATED_DATE", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date createdDate;

    @JsonProperty("updatedDate")
    @Column(name = "UPDATED_DATE", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updatedDate;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getId() {
        return workflowRequestId;
    }

    @Override
    public void setId(String id) {
        this.workflowRequestId = id;
    }

    @Override
    public Tenant getTenant() {
        return this.tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getExternalSystemId() {
        return externalSystemId;
    }

    public void setExternalSystemId(String externalSystemId) {
        this.externalSystemId = externalSystemId;
    }

    public String getWorkflowRequestId() {
        return workflowRequestId;
    }

    public void setWorkflowRequestId(String workflowRequestId) {
        this.workflowRequestId = workflowRequestId;
    }

    public String getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(String sourceFile) {
        this.sourceFile = sourceFile;
    }

    public String getErrorFile() {
        return errorFile;
    }

    public void setErrorFile(String errorFile) {
        this.errorFile = errorFile;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getEventSubmittedTime() {
        return eventSubmittedTime;
    }

    public void setEventSubmittedTime(Date eventSubmittedTime) {
        this.eventSubmittedTime = eventSubmittedTime;
    }

    public Date getEventStartedTime() {
        return eventStartedTime;
    }

    public void setEventStartedTime(Date eventStartedTime) {
        this.eventStartedTime = eventStartedTime;
    }

    public Date getEventCompletedTime() {
        return eventCompletedTime;
    }

    public void setEventCompletedTime(Date eventCompletedTime) {
        this.eventCompletedTime = eventCompletedTime;
    }

    public Date getCreated() {
        return createdDate;
    }

    public void setCreated(Date created) {
        this.createdDate = created;
    }

    public Date getUpdated() {
        return updatedDate;
    }

    public void setUpdated(Date updated) {
        this.updatedDate = updated;
    }

}
