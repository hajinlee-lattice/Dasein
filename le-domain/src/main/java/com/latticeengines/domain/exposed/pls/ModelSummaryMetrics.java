package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "MODELQUALITY_SUMMARY_METRICS", uniqueConstraints = { @UniqueConstraint(columnNames = { "PID" }),
        @UniqueConstraint(columnNames = { "TENANT_NAME", "NAME" }) })
@Filter(name = "tenantFilter", condition = "TENANT_NAME = :tenantFilterName")
public class ModelSummaryMetrics implements HasPid, HasName {
    private String tenantName;
    private String name;
    private Long pid;
    private Double rocScore;
    private Double top20PercentLift;
    private String dataCloudVersion;
    private Long lastUpdateTime;
    private Long constructionTime;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID")
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("TenantName")
    @Column(name = "TENANT_NAME", nullable = false)
    @Index(name = "MODELQUALITY_SUMMARY_NAME_IDX")
    public String getTenantName() {
        return tenantName;
    }

    @JsonProperty("TenantName")
    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    @Override
    @JsonProperty("Name")
    @Column(name = "NAME", nullable = false)
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("Name")
    public void setName(String name) {
        this.name = name;
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
