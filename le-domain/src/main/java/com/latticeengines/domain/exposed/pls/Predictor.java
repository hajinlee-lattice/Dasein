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

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "PREDICTOR")
public class Predictor implements HasName, HasPid, HasTenant {

    private Long pid;
    private String name;
    private String displayName;
    private String approvedUsage;
    private String category;
    private String fundamentalType;
    private Double uncertaintyCoefficient;
    private ModelSummary modelSummary;
    private List<PredictorElement> predictorElements = new ArrayList<>();
    private Tenant tenant;
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PREDICTOR_PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @Column(name = "NAME", nullable = false)
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Column(name = "DISPLAY_NAME", nullable = false)
    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Column(name = "APPROVED_USAGE", nullable = true)
    public String getApprovedUsage() {
        return approvedUsage;
    }

    public void setApprovedUsage(String approvedUsage) {
        this.approvedUsage = approvedUsage;
    }

    @Column(name = "CATEGORY", nullable = false)
    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Column(name = "FUNDAMENTAL_TYPE", nullable = true)
    public String getFundamentalType() {
        return fundamentalType;
    }

    public void setFundamentalType(String fundamentalType) {
        this.fundamentalType = fundamentalType;
    }

    @Column(name = "UNCERTAINTY_COEFF", nullable = true)
    public Double getUncertaintyCoefficient() {
        return uncertaintyCoefficient;
    }

    public void setUncertaintyCoefficient(Double uncertaintyCoefficient) {
        this.uncertaintyCoefficient = uncertaintyCoefficient;
    }

    @ManyToOne(cascade = { CascadeType.MERGE, CascadeType.REMOVE })
    @JoinColumn(name = "FK_MODELSUMMARY_ID", nullable = false)
    public ModelSummary getModelSummary() {
        return modelSummary;
    }

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
        predictorElement.setTenant(getTenant());
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @Override
    @JsonProperty("Tenant")
    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Tenant getTenant() {
        return tenant;
    }
}
