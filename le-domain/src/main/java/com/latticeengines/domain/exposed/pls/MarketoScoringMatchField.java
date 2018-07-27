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

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "MARKETO_SCORING_MATCH_FIELD")
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class MarketoScoringMatchField implements HasPid, HasTenant {

    private Long pid;
    private Tenant tenant;
    private String modelFieldName;
    private String marketoFieldName;
    private ScoringRequestConfig scoringRequestConfig;

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
    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @Column(name = "MODEL_FIELD_NAME", nullable = true)
    @JsonProperty("modelFieldName")
    public String getModelFieldName() {
        return modelFieldName;
    }

    public void setModelFieldName(String modelFieldName) {
        this.modelFieldName = modelFieldName;
    }

    @Column(name = "MARKETO_FIELD_NAME", nullable = true)
    @JsonProperty("marketoFieldName")
    public String getMarketoFieldName() {
        return marketoFieldName;
    }

    public void setMarketoFieldName(String marketoFieldName) {
        this.marketoFieldName = marketoFieldName;
    }

    @ManyToOne
    @JoinColumn(name = "SCORING_REQUEST_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    public ScoringRequestConfig getScoringRequestConfig() {
        return scoringRequestConfig;
    }

    public void setScoringRequestConfig(ScoringRequestConfig scoringRequestConfig) {
        this.scoringRequestConfig = scoringRequestConfig;
    }

}
