package com.latticeengines.domain.exposed.serviceapps.cdl;

import java.io.Serializable;
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
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "CDL_BUSINESS_CALENDAR")
@Filters({ @Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId") })
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class BusinessCalendar implements HasPid, HasTenant, HasAuditingFields, Serializable {

    private static final long serialVersionUID = -8569929129948247705L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonIgnore
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "CREATED", nullable = false)
    @JsonProperty("created")
    private Date created;

    @Column(name = "UPDATED", nullable = false)
    @JsonProperty("updated")
    private Date updated;

    @Enumerated(EnumType.STRING)
    @Column(name = "MODE", nullable = false)
    @JsonProperty("mode")
    private Mode mode;

    @Column(name = "STARTING_DATE", length = 6)
    @JsonProperty("startingDate")
    private String startingDate;

    @Column(name = "STARTING_DAY", length = 11)
    @JsonProperty("startingDay")
    private String startingDay;

    @Column(name = "UPDATED_BY")
    @JsonProperty("updatedBy")
    private String updatedBy;

    @Column(name = "LONGER_MONTH")
    @JsonProperty("longerMonth")
    private Integer longerMonth;

    @Transient
    @JsonProperty("evaluationYear")
    private Integer evaluationYear;

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

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public String getStartingDate() {
        return startingDate;
    }

    public void setStartingDate(String startingDate) {
        this.startingDate = startingDate;
    }

    public String getStartingDay() {
        return startingDay;
    }

    public void setStartingDay(String startingDay) {
        this.startingDay = startingDay;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public Integer getEvaluationYear() {
        return evaluationYear;
    }

    public void setEvaluationYear(Integer evaluationYear) {
        this.evaluationYear = evaluationYear;
    }

    public Integer getLongerMonth() {
        return longerMonth;
    }

    public void setLongerMonth(Integer longerMonth) {
        this.longerMonth = longerMonth;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public enum Mode {
        STARTING_DATE, STARTING_DAY, STANDARD
    }
}
