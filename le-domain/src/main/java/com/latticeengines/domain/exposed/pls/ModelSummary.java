package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

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
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "MODEL_SUMMARY", 
       uniqueConstraints = { @UniqueConstraint(columnNames = { "Id" }), @UniqueConstraint(columnNames = { "Name" }) })
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class ModelSummary implements HasId<String>, HasName, HasPid, HasTenant, HasTenantId {
    
    private String id;
    private String name;
    private Long pid;
    private Tenant tenant;
    private Long tenantId;
    private List<Predictor> predictors = new ArrayList<>();
    private Double rocScore;
    private String lookupId;
    private Boolean downloaded = false;
    private Long trainingRowCount;
    private Long testRowCount;
    private Long totalRowCount;
    private Long trainingConversionCount;
    private Long testConversionCount;
    private Long totalConversionCount;
    private KeyValue details;
    private Long constructionTime;
    private Boolean active = false;

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
    @JsonProperty("name")
    @Column(name = "NAME", unique = true, nullable = false)
    @Index(name = "MODEL_SUMMARY_NAME_IDX")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("Name")
    public void setName(String name) {
        this.name = name;
    }

    @Override
    @JsonProperty("Id")
    @Column(name = "ID", unique = true, nullable = false)
    @Index(name = "MODEL_SUMMARY_ID_IDX")
    public String getId() {
        return id;
    }

    @Override
    @JsonProperty("Id")
    public void setId(String id) {
        this.id = id;
    }

    @Override
    @JsonProperty("Tenant")
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        setTenantId(tenant.getPid());
    }

    @Override
    @JsonProperty("Tenant")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Tenant getTenant() {
        return tenant;
    }
    
    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "modelSummary")
    @OnDelete(action = OnDeleteAction.CASCADE)
    public List<Predictor> getPredictors() {
        return predictors;
    }

    public void setPredictors(List<Predictor> predictors) {
        this.predictors = predictors;
    }
    
    public void addPredictor(Predictor predictor) {
        if (predictor != null) {
            predictors.add(predictor);
            predictor.setModelSummary(this);
            predictor.setTenantId(getTenantId());
        }
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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonProperty("RocScore")
    @Column(name = "ROC_SCORE", nullable = false)
    public Double getRocScore() {
        return rocScore;
    }

    @JsonProperty("RocScore")
    public void setRocScore(Double rocScore) {
        this.rocScore = rocScore;
    }

    @JsonIgnore
    @Column(name = "LOOKUP_ID", nullable = false)
    public String getLookupId() {
        return lookupId;
    }

    @JsonIgnore
    public void setLookupId(String lookupId) {
        this.lookupId = lookupId;
    }

    @JsonIgnore
    @Column(name = "DOWNLOADED", nullable = false)
    public Boolean getDownloaded() {
        return downloaded;
    }

    @JsonIgnore
    public void setDownloaded(Boolean downloaded) {
        this.downloaded = downloaded;
    }

    @JsonProperty("TrainingRowCount")
    @Column(name = "TRAINING_ROW_COUNT", nullable = false)
    public Long getTrainingRowCount() {
        return trainingRowCount;
    }

    @JsonProperty("TrainingRowCount")
    public void setTrainingRowCount(Long trainingRowCount) {
        this.trainingRowCount = trainingRowCount;
    }

    @JsonProperty("TestRowCount")
    @Column(name = "TEST_ROW_COUNT", nullable = false)
    public Long getTestRowCount() {
        return testRowCount;
    }

    @JsonProperty("TestRowCount")
    public void setTestRowCount(Long testRowCount) {
        this.testRowCount = testRowCount;
    }

    @JsonProperty("TotalRowCount")
    @Column(name = "TOTAL_ROW_COUNT", nullable = false)
    public Long getTotalRowCount() {
        return totalRowCount;
    }

    @JsonProperty("TotalRowCount")
    public void setTotalRowCount(Long totalRowCount) {
        this.totalRowCount = totalRowCount;
    }

    @JsonProperty("TrainingConversionCount")
    @Column(name = "TRAINING_CONVERSION_COUNT", nullable = false)
    public Long getTrainingConversionCount() {
        return trainingConversionCount;
    }

    @JsonProperty("TrainingConversionCount")
    public void setTrainingConversionCount(Long trainingConversionCount) {
        this.trainingConversionCount = trainingConversionCount;
    }

    @JsonProperty("TestConversionCount")
    @Column(name = "TEST_CONVERSION_COUNT", nullable = false)
    public Long getTestConversionCount() {
        return testConversionCount;
    }

    @JsonProperty("TestConversionCount")
    public void setTestConversionCount(Long testConversionCount) {
        this.testConversionCount = testConversionCount;
    }

    @JsonProperty("TotalConversionCount")
    @Column(name = "TOTAL_CONVERSION_COUNT", nullable = false)
    public Long getTotalConversionCount() {
        return totalConversionCount;
    }

    @JsonProperty("TotalConversionCount")
    public void setTotalConversionCount(Long totalConversionCount) {
        this.totalConversionCount = totalConversionCount;
    }

    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "FK_KEY_VALUE_ID", nullable = false)
    @JsonProperty("Details")
    public KeyValue getDetails() {
        return details;
    }

    @JsonProperty("Details")
    public void setDetails(KeyValue details) {
        this.details = details;
        if (details != null) {
            details.setTenantId(getTenantId());
        }
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

    @JsonProperty("Active")
    @Column(name = "ACTIVE", nullable = false)
    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

}
