package com.latticeengines.domain.exposed.cdl;

import static javax.persistence.TemporalType.TIMESTAMP;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "MOCK_BROKER_INSTANCE", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"SOURCE_ID", "FK_TENANT_ID"})})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MockBrokerInstance implements HasPid, HasAuditingFields, HasTenant, Serializable {

    private static final long serialVersionUID = -8905155098162859912L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "SOURCE_ID", nullable = false)
    @JsonProperty("sourceId")
    private String sourceId;

    @Column(name = "DISPLAY_NAME", nullable = false)
    @JsonProperty("displayName")
    private String displayName;

    @JsonProperty("dataStreamId")
    @Column(name = "DATA_STREAM_ID")
    private String dataStreamId;

    @JsonProperty("selectedFields")
    @Column(name = "SELECTED_FIELDS", columnDefinition = "'JSON'")
    @Type(type = "json")
    private List<String> selectedFields = new ArrayList<>();

    @JsonProperty("ingestionScheduler")
    @Column(name = "INGESTION_SCHEDULER", columnDefinition = "'JSON'")
    @Type(type = "json")
    private IngestionScheduler ingestionScheduler;

    @JsonProperty("documentType")
    @Column(name = "DOCUMENT_TYPE", nullable = false)
    private String documentType;

    @Temporal(TIMESTAMP)
    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    private Date created;

    @Temporal(TIMESTAMP)
    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    private Date updated;

    @JsonProperty("active")
    @Column(name = "ACTIVE", nullable = false)
    private Boolean active = false;

    @Temporal(TIMESTAMP)
    @Column(name = "NEXT_SCHEDULED_TIME")
    @JsonProperty("nextScheduledTime")
    private Date nextScheduledTime;

    @JsonProperty("lastAggregationWorkflowId")
    @Column(name = "LAST_AGGREGATION_WORKFLOW_ID")
    private Long lastAggregationWorkflowId;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date created) {
        this.created = created;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public IngestionScheduler getIngestionScheduler() {
        return ingestionScheduler;
    }

    public void setIngestionScheduler(IngestionScheduler ingestionScheduler) {
        this.ingestionScheduler = ingestionScheduler;
    }

    public String getDataStreamId() {
        return dataStreamId;
    }

    public void setDataStreamId(String dataStreamId) {
        this.dataStreamId = dataStreamId;
    }

    public List<String> getSelectedFields() {
        return selectedFields;
    }

    public void setSelectedFields(List<String> selectedFields) {
        this.selectedFields = selectedFields;
    }

    public String getDocumentType() {
        return documentType;
    }

    public void setDocumentType(String documentType) {
        this.documentType = documentType;
    }


    public Date getNextScheduledTime() {
        return nextScheduledTime;
    }

    public void setNextScheduledTime(Date nextScheduledTime) {
        this.nextScheduledTime = nextScheduledTime;
    }

    public Long getLastAggregationWorkflowId() {
        return lastAggregationWorkflowId;
    }

    public void setLastAggregationWorkflowId(Long lastAggregationWorkflowId) {
        this.lastAggregationWorkflowId = lastAggregationWorkflowId;
    }
}
