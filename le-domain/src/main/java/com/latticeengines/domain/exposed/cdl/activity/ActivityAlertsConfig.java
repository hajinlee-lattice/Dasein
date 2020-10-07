package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
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
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "ACTIVITY_ALERTS_CONFIG", uniqueConstraints = { //
        @UniqueConstraint(columnNames = { "NAME", "FK_TENANT_ID" }) })
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ActivityAlertsConfig implements HasPid, HasTenant, Serializable, HasAuditingFields {

    private static final long serialVersionUID = 0L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = CascadeType.REMOVE)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "NAME", nullable = false)
    @JsonProperty("name")
    private String name;

    @Column(name = "ALERT_HEADER", nullable = false)
    @JsonProperty("alert_header")
    private String alertHeader;

    @Column(name = "ALERT_CATEGORY", nullable = false)
    @JsonProperty("alert_category")
    @Enumerated(EnumType.STRING)
    private AlertCategory alertCategory;

    @Column(name = "QUALIFICATION_PERIOD_DAYS", nullable = false)
    @JsonProperty("qualification_period_days")
    private long qualificationPeriodDays;

    @Column(name = "ALERT_MESSAGE_TEMPLATE", nullable = false, length = 1000)
    @JsonProperty("alert_message_template")
    private String alertMessageTemplate;

    @Column(name = "IS_ACTIVE", nullable = false)
    @JsonProperty("is_active")
    private boolean isActive;

    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("created")
    private Date created;

    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("updated")
    private Date updated;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlertHeader() {
        return alertHeader;
    }

    public void setAlertHeader(String alertHeader) {
        this.alertHeader = alertHeader;
    }

    public AlertCategory getAlertCategory() {
        return alertCategory;
    }

    public void setAlertCategory(AlertCategory alertCategory) {
        this.alertCategory = alertCategory;
    }

    public long getQualificationPeriodDays() {
        return qualificationPeriodDays;
    }

    public void setQualificationPeriodDays(long qualificationPeriodDays) {
        this.qualificationPeriodDays = qualificationPeriodDays;
    }

    public String getAlertMessageTemplate() {
        return alertMessageTemplate;
    }

    public void setAlertMessageTemplate(String alertMessageTemplate) {
        this.alertMessageTemplate = alertMessageTemplate;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
    }
}
