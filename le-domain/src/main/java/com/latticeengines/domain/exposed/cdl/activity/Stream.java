package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
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
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.Tenant;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Table(name = "ATLAS_STREAM", uniqueConstraints = { //
        @UniqueConstraint(columnNames = { "NAME", "FK_TENANT_ID" }) })
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class Stream implements HasPid, Serializable, HasAuditingFields {

    private static final long serialVersionUID = 7473595458075796126L;

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

    @JsonProperty("match_entities")
    @Type(type = "json")
    @Column(name = "MATCH_ENTITIES", columnDefinition = "'JSON'", nullable = false)
    private List<String> matchEntities;

    @JsonProperty("aggr_entities")
    @Type(type = "json")
    @Column(name = "AGGR_ENTITIES", columnDefinition = "'JSON'", nullable = false)
    private List<String> aggrEntities;

    @JsonProperty("date_attribute")
    @Column(name = "DATE_ATTRIBUTE", length = 50, nullable = false)
    private String dateAttribute;

    @JsonProperty("periods")
    @Type(type = "json")
    @Column(name = "PERIODS", columnDefinition = "'JSON'", nullable = false)
    private List<PeriodStrategy.Template> periods;

    // if not provided, never delete
    @JsonProperty("retention_days")
    @Column(name = "RETENTION_DAYS")
    private Integer retentionDays;

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "stream")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
    private List<Dimension> dimensions;

    // configuration to derive attributes for stream
    @JsonProperty("attribute_derivers")
    @Type(type = "json")
    @Column(name = "ATTRIBUTE_DERIVER", columnDefinition = "'JSON'")
    private List<StreamAttributeDeriver> attributeDerivers;

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

    public List<String> getMatchEntities() {
        return matchEntities;
    }

    public void setMatchEntities(List<String> matchEntities) {
        this.matchEntities = matchEntities;
    }

    public void setAggrEntities(List<String> aggrEntities) {
        this.aggrEntities = aggrEntities;
    }

    public List<String> getAggrEntities() {
        return aggrEntities;
    }

    public String getDateAttribute() {
        return dateAttribute;
    }

    public void setDateAttribute(String dateAttribute) {
        this.dateAttribute = dateAttribute;
    }

    public List<PeriodStrategy.Template> getPeriods() {
        return periods;
    }

    public void setPeriods(List<PeriodStrategy.Template> periods) {
        this.periods = periods;
    }

    public Integer getRetentionDays() {
        return retentionDays;
    }

    public void setRetentionDays(Integer retentionDays) {
        this.retentionDays = retentionDays;
    }

    public List<Dimension> getDimensions() {
        return dimensions;
    }

    public List<StreamAttributeDeriver> getAttributeDerivers() {
        return attributeDerivers;
    }

    public void setAttributeDerivers(List<StreamAttributeDeriver> attributeDerivers) {
        this.attributeDerivers = attributeDerivers;
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
