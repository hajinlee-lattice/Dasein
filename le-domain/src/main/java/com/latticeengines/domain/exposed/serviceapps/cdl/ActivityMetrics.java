package com.latticeengines.domain.exposed.serviceapps.cdl;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

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
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "CDL_ACTIVITY_METRICS")
@Filters({ @Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId") })
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class ActivityMetrics implements HasPid, HasTenant, HasAuditingFields, Serializable {

    private static final long serialVersionUID = -5942157947113415428L;

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
    @Column(name = "METRICS", nullable = false, length = 100)
    @JsonProperty("metrics")
    private InterfaceName metrics;

    @JsonIgnore
    @Column(name = "PERIODS", nullable = false, length = 1000)
    private String periods; // For DB

    @JsonProperty("periods")
    @Transient
    private List<TimeFilter> periodsConfig; // For Application

    @Column(name = "IS_EOL")
    @JsonProperty("IsEOL")
    private boolean isEOL;

    @Column(name = "DEPRECATED")
    @JsonProperty("deprecated")
    private Date deprecated;

    @Enumerated(EnumType.STRING)
    @Column(name = "TYPE", nullable = false, length = 100)
    @JsonProperty("type")
    private ActivityType type;

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

    public InterfaceName getMetrics() {
        return metrics;
    }

    public void setMetrics(InterfaceName metrics) {
        this.metrics = metrics;
    }

    public void setPeriods() {
        if (periodsConfig != null) {
            periods = JsonUtils.serialize(periodsConfig);
        }
    }

    public List<TimeFilter> getPeriodsConfig() {
        if (periodsConfig == null) {
            List<?> list = JsonUtils.deserialize(periods, List.class);
            periodsConfig = JsonUtils.convertList(list, TimeFilter.class);
        }
        return periodsConfig;
    }

    public void setPeriodsConfig(List<TimeFilter> periodsConfig) {
        this.periodsConfig = periodsConfig;
        this.periods = JsonUtils.serialize(periodsConfig);
    }

    public boolean isEOL() {
        return isEOL;
    }

    public void setEOL(boolean isEOL) {
        this.isEOL = isEOL;
    }

    public Date getDeprecated() {
        return deprecated;
    }

    public void setDeprecated(Date deprecated) {
        this.deprecated = deprecated;
    }

    public ActivityType getType() {
        return type;
    }

    public void setType(ActivityType type) {
        this.type = type;
    }

}
