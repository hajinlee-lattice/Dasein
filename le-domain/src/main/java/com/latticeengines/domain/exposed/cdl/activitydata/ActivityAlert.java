package com.latticeengines.domain.exposed.cdl.activitydata;

import java.util.Date;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Convert;
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

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.FilterDefs;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.ParamDef;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Table(name = "ActivityAlert", //
        uniqueConstraints = @UniqueConstraint(columnNames = { "ENTITY_ID", "ENTITY_TYPE", "TENANT_ID", "VERSION",
                "CREATION_TIMESTAMP" }), //
        indexes = { @Index(name = "REC_CREATION_TIMESTAMP", columnList = "CREATION_TIMESTAMP"), //
                @Index(name = "REC_VERSION", columnList = "VERSION"), //
                @Index(name = "REC_CATEGORY", columnList = "CATEGORY") })
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class), })
@FilterDefs(@FilterDef(name = "tenantFilter", defaultCondition = "TENANT_ID = :tenantFilterId", parameters = {
        @ParamDef(name = "tenantFilterId", type = "java.lang.Long") }))
@Filters(@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId"))
@JsonIgnoreProperties(ignoreUnknown = true)
public class ActivityAlert implements HasPid, HasTenantId {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @JsonProperty("pid")
    private Long pid;

    @Column(name = "ENTITY_ID", nullable = false)
    @JsonProperty("entity_id")
    private String entityId;

    @Column(name = "ENTITY_TYPE", nullable = false)
    @JsonProperty("entity_type")
    @Enumerated(EnumType.STRING)
    private BusinessEntity entityType;

    @Column(name = "TENANT_ID", nullable = false)
    @JsonProperty("tenant_id")
    private Long tenantId;

    @Column(name = "CREATION_TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("creationTimestamp")
    private Date creationTimestamp;

    @Column(name = "VERSION", nullable = false)
    @JsonProperty("version")
    private String version;

    @Column(name = "ALERT_NAME", nullable = false)
    @JsonProperty("alertName")
    private String alertName;

    @Column(name = "CATEGORY", nullable = false)
    @JsonProperty("category")
    @Enumerated(EnumType.STRING)
    private AlertCategory category;

    @Column(name = "ALERT_DATA", columnDefinition = "'JSON'")
    @Type(type = "json")
    @JsonProperty("alert_data")
    @Convert(attributeName = "data")
    protected Map<String, Object> alertData;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public Date getCreationTimestamp() {
        return creationTimestamp;
    }

    public void setCreationTimestamp(Date creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public BusinessEntity getEntityType() {
        return entityType;
    }

    public void setEntityType(BusinessEntity entityType) {
        this.entityType = entityType;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getAlertName() {
        return alertName;
    }

    public void setAlertName(String alertName) {
        this.alertName = alertName;
    }

    public AlertCategory getCategory() {
        return category;
    }

    public void setCategory(AlertCategory category) {
        this.category = category;
    }

    public Map<String, Object> getAlertData() {
        return alertData;
    }

    public void setAlertData(Map<String, Object> alertData) {
        this.alertData = alertData;
    }
}
