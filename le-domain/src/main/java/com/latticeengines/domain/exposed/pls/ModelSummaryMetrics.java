package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "MODELQUALITY_SUMMARY_METRICS", uniqueConstraints = { @UniqueConstraint(columnNames = { "ID" }),
        @UniqueConstraint(columnNames = { "NAME", "TENANT_ID" }) })
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class ModelSummaryMetrics implements HasId<String>, HasPid, HasTenant, HasName, HasTenantId {
    private String id;
    private Tenant tenant;
    private Long pid;
    private String name;
    private Long tenantId;
    private Double rocScore;
    private Double top20PercentLift;
    private String dataCloudVersion;
    private Long lastUpdateTime;
    private Long constructionTime;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @JsonProperty("Id")
    @Column(name = "ID", unique = true, nullable = false)
    @Index(name = "MODELQUALITY_SUMMARY_NAME_IDX")
    public String getId() {
        return id;
    }

    @Override
    @JsonProperty("Id")
    public void setId(String id) {
        this.id = id;
    }

    @Override
    @JsonProperty("Name")
    @Column(name = "NAME", nullable = false)
    @Index(name = "MODELQUALITY_SUMMARY_NAME_IDX")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("Name")
    public void setName(String name) {
        this.name = name;
    }

    @Override
    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @JsonProperty("RocScore")
    @Column(name = "ROC_SCORE", nullable = false)
    @Type(type = "com.latticeengines.db.exposed.extension.NaNSafeDoubleType")
    public Double getRocScore() {
        return rocScore;
    }

    @JsonProperty("RocScore")
    public void setRocScore(Double rocScore) {
        this.rocScore = rocScore;
    }

    @JsonProperty("Top20PctLift")
    @Column(name = "TOP_20_PCT_LIFT", nullable = true)
    @Type(type = "com.latticeengines.db.exposed.extension.NaNSafeDoubleType")
    public Double getTop20PercentLift() {
        return top20PercentLift;
    }

    @JsonProperty("Top20PctLift")
    public void setTop20PercentLift(Double top20PercentLift) {
        this.top20PercentLift = top20PercentLift;
    }

    @JsonProperty("DataCloudVersion")
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    @JsonProperty("DataCloudVersion")
    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    @Override
    @JsonProperty("Tenant")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    @JsonProperty("Tenant")
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        setTenantId(tenant.getPid());
    }

    @JsonProperty("LastUpdateTime")
    @Column(name = "LAST_UPDATE_TIME", nullable = false)
    public Long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @JsonProperty("LastUpdateTime")
    public void setLastUpdateTime(Long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @JsonProperty("ConstructionTime")
    @Column(name = "CONSTRUCTION_TIME", nullable = false)
    public Long getConstructionTime() {
        return constructionTime;
    }

    @JsonProperty("ConstructionTime")
    public void setConstructionTime(Long constructionTime) {
        this.constructionTime = constructionTime;
    }

}
