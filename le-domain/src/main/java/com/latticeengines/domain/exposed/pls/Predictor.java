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
import javax.persistence.Table;

import org.apache.commons.lang.ObjectUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenantId;

@Entity
@Table(name = "PREDICTOR")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class Predictor implements HasName, HasPid, HasTenantId, Comparable<Predictor> {

    private Long pid;
    private String name;
    private String displayName;
    private String approvedUsage;
    private String category;
    private String fundamentalType;
    private Double uncertaintyCoefficient;
    private ModelSummary modelSummary;
    private List<PredictorElement> predictorElements = new ArrayList<>();
    private Long tenantId;
    private Boolean usedForBuyerInsights = false;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
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

    @Column(name = "DISPLAY_NAME")
    @JsonProperty("DisplayName")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("DisplayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Column(name = "APPROVED_USAGE", nullable = true)
    @JsonProperty("ApprovedUsage")
    public String getApprovedUsage() {
        return approvedUsage;
    }

    @JsonProperty("ApprovedUsage")
    public void setApprovedUsage(String approvedUsage) {
        this.approvedUsage = approvedUsage;
    }

    @Column(name = "CATEGORY")
    @JsonProperty("Category")
    public String getCategory() {
        return category;
    }

    @JsonProperty("Category")
    public void setCategory(String category) {
        this.category = category;
    }

    @Column(name = "FUNDAMENTAL_TYPE", nullable = true)
    @JsonProperty("FundamentalType")
    public String getFundamentalType() {
        return fundamentalType;
    }

    @JsonProperty("FundamentalType")
    public void setFundamentalType(String fundamentalType) {
        this.fundamentalType = fundamentalType;
    }

    @Column(name = "UNCERTAINTY_COEFF", nullable = true)
    @JsonProperty("UncertaintyCoefficient")
    public Double getUncertaintyCoefficient() {
        return uncertaintyCoefficient;
    }

    @JsonProperty("UncertaintyCoefficient")
    public void setUncertaintyCoefficient(Double uncertaintyCoefficient) {
        this.uncertaintyCoefficient = uncertaintyCoefficient;
    }

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE, CascadeType.REMOVE })
    @JoinColumn(name = "FK_MODELSUMMARY_ID", nullable = false)
    public ModelSummary getModelSummary() {
        return modelSummary;
    }

    @JsonIgnore
    public void setModelSummary(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
    }

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "predictor")
    @OnDelete(action = OnDeleteAction.CASCADE)
    public List<PredictorElement> getPredictorElements() {
        return predictorElements;
    }

    public void setPredictorElements(List<PredictorElement> predictorElements) {
        this.predictorElements = predictorElements;
    }

    public void addPredictorElement(PredictorElement predictorElement) {
        predictorElements.add(predictorElement);
        predictorElement.setPredictor(this);
        predictorElement.setTenantId(getTenantId());
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

    @Column(name = "USED_FOR_BI", nullable = false)
    @JsonProperty("UsedForBuyerInsights")
    public Boolean getUsedForBuyerInsights() {
        return usedForBuyerInsights;
    }

    @JsonProperty("UsedForBuyerInsights")
    public void setUsedForBuyerInsights(Boolean usedForBuyerInsights) {
        this.usedForBuyerInsights = usedForBuyerInsights;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public int compareTo(Predictor predictor) {
        // descending order
        return ObjectUtils.compare(
                predictor != null && predictor.getUncertaintyCoefficient() != null ? predictor
                        .getUncertaintyCoefficient() : null,
                this.uncertaintyCoefficient != null ? this.uncertaintyCoefficient : null);
    }
}
