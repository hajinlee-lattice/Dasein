package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.persistence.Basic;
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
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "ATLAS_STREAM", uniqueConstraints = { //
        @UniqueConstraint(columnNames = { "NAME", "FK_TENANT_ID" }) })
public class Stream implements HasPid, Serializable, HasAuditingFields {

    private static final long serialVersionUID = 7473595458075796126L;
    private static final String SEPERATOR = ",";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("tenant")
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    // For DB, entity names concatenated by comma
    // eg. Account,Contact
    @JsonIgnore
    @Column(name = "MATCH_ENTITIES", nullable = false)
    private String matchEntityConfig;

    @JsonProperty("match_entities")
    @Transient
    private List<String> matchEntities;

    // For DB, entity names concatenated by comma
    // eg. Account,Contact
    @JsonIgnore
    @Column(name = "AGGR_ENTITIES", nullable = false)
    private String aggrEntityConfig;

    @JsonProperty("aggr_entities")
    @Transient
    private List<String> aggrEntities;

    @JsonProperty("date_attribute")
    @Enumerated(EnumType.STRING)
    @Column(name = "DATE_ATTRIBUTE", length = 50, nullable = false)
    private InterfaceName dateAttribute;

    // For DB, period names concatenated by comma
    // eg. Week,Month,Quarter,Year
    @JsonIgnore
    @Column(name = "PERIODS", nullable = false)
    private String periodConfig;

    @JsonProperty("periods")
    @Transient
    private List<PeriodStrategy.Template> periods;

    // if not provided, never delete
    @JsonProperty("retention_days")
    @Column(name = "RETENTION_DAYS")
    private Integer retentionDays;

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "stream")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
    private List<Dimension> dimensions;

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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public void setMatchEntityConfig(String matchEntityConfig) {
        this.matchEntityConfig = matchEntityConfig;
        this.matchEntities = matchEntityConfig == null ? new ArrayList<>()
                : Arrays.asList(matchEntityConfig.split(SEPERATOR));
    }

    public List<String> getMatchEntities() {
        if (matchEntities != null) {
            return matchEntities;
        }
        this.matchEntities = matchEntityConfig == null ? new ArrayList<>()
                : Arrays.asList(matchEntityConfig.split(SEPERATOR));
        return matchEntities;
    }

    public void setMatchEntities(List<String> matchEntities) {
        this.matchEntities = matchEntities;
        this.matchEntityConfig = matchEntities == null ? null : String.join(SEPERATOR, matchEntities);
    }

    public void setAggrEntityConfig(String aggrEntityConfig) {
        this.aggrEntityConfig = aggrEntityConfig;
        this.aggrEntities = aggrEntityConfig == null ? new ArrayList<>()
                : Arrays.asList(aggrEntityConfig.split(SEPERATOR));
    }

    public void setAggrEntities(List<String> aggrEntities) {
        this.aggrEntities = aggrEntities;
        this.aggrEntityConfig = aggrEntities == null ? null : String.join(SEPERATOR, aggrEntities);
    }

    public List<String> getAggrEntities() {
        if (aggrEntities != null) {
            return aggrEntities;
        }
        this.aggrEntities = aggrEntityConfig == null ? new ArrayList<>()
                : Arrays.asList(aggrEntityConfig.split(SEPERATOR));
        return aggrEntities;
    }

    public InterfaceName getDateAttribute() {
        return dateAttribute;
    }

    public void setDateAttribute(InterfaceName dateAttribute) {
        this.dateAttribute = dateAttribute;
    }

    public void setPeriodConfig(String periodConfig) {
        this.periodConfig = periodConfig;
        this.periods = Arrays.stream(periodConfig.split(SEPERATOR)) //
                .map(PeriodStrategy.Template::fromName) //
                .collect(Collectors.toList());
    }

    public List<PeriodStrategy.Template> getPeriods() {
        if (periods != null) {
            return periods;
        }
        this.periods = Arrays.stream(periodConfig.split(SEPERATOR)) //
                .map(PeriodStrategy.Template::fromName) //
                .collect(Collectors.toList());
        return this.periods;
    }

    public void setPeriods(List<PeriodStrategy.Template> periods) {
        this.periods = periods;
        this.periodConfig = String.join(SEPERATOR,
                periods.stream().map(PeriodStrategy.Template::name).collect(Collectors.toList()));
    }

    public Integer getRetentionDays() {
        return retentionDays;
    }

    public void setRetentionDays(Integer retentionDays) {
        this.retentionDays = retentionDays;
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

}
