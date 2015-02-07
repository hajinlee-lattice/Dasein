package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenantId;

@Entity
@Table(name = "PREDICTOR_ELEMENT")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class PredictorElement implements HasPid, HasName, HasTenantId {
    
    private String name;
    private Long pid;
    private Integer correlationSign;
    private Long count;
    private Double lift;
    private Double lowerInclusive;
    private Double upperExclusive;
    private Double uncertaintyCoefficient;
    private String values;
    private Double revenue;
    private Boolean visible;
    private Predictor predictor;
    private Long tenantId;

    @Override
    @Column(name = "NAME", nullable = false)
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

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

    @Column(name = "CORRELATION_SIGN", nullable = false)
    public Integer getCorrelationSign() {
        return correlationSign;
    }

    public void setCorrelationSign(Integer correlationSign) {
        this.correlationSign = correlationSign;
    }

    @Column(name = "COUNT", nullable = false)
    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Column(name = "LIFT", nullable = false)
    public Double getLift() {
        return lift;
    }

    public void setLift(Double lift) {
        this.lift = lift;
    }

    @Column(name = "LOWER_INCLUSIVE", nullable = true)
    public Double getLowerInclusive() {
        return lowerInclusive;
    }

    public void setLowerInclusive(Double lowerInclusive) {
        this.lowerInclusive = lowerInclusive;
    }

    @Column(name = "UPPER_EXCLUSIVE", nullable = true)
    public Double getUpperExclusive() {
        return upperExclusive;
    }

    public void setUpperExclusive(Double upperExclusive) {
        this.upperExclusive = upperExclusive;
    }

    @Column(name = "UNCERTAINTY_COEFF", nullable = false)
    public Double getUncertaintyCoefficient() {
        return uncertaintyCoefficient;
    }

    public void setUncertaintyCoefficient(Double uncertaintyCoefficient) {
        this.uncertaintyCoefficient = uncertaintyCoefficient;
    }

    @Column(name = "VALUES", nullable = true)
    @Lob
    public String getValues() {
        return values;
    }

    public void setValues(String values) {
        this.values = values;
    }

    @Column(name = "REVENUE", nullable = false)
    public Double getRevenue() {
        return revenue;
    }

    public void setRevenue(Double revenue) {
        this.revenue = revenue;
    }

    @Column(name = "VISIBLE", nullable = false)
    public Boolean getVisible() {
        return visible;
    }

    public void setVisible(Boolean visible) {
        this.visible = visible;
    }

    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_PREDICTOR_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    public Predictor getPredictor() {
        return predictor;
    }

    @JsonIgnore
    public void setPredictor(Predictor predictor) {
        this.predictor = predictor;
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

}
