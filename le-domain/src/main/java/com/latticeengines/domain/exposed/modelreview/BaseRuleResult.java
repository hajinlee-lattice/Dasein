package com.latticeengines.domain.exposed.modelreview;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@MappedSuperclass
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public abstract class BaseRuleResult implements HasPid, HasTenant {

    @JsonIgnore
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    protected Long pid;

    @JsonProperty
    @Transient
    protected int flaggedItemCount;

    @JsonProperty
    @Column(name = "RULENAME", nullable = false)
    protected String dataRuleName;

    @JsonProperty
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty
    @Column(name = "MODEL_ID", nullable = false)
    @Index(name = "IX_MODEL_ID")
    private String modelId;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    public int getFlaggedItemCount() {
        return flaggedItemCount;
    }

    public void setFlaggedItemCount(int flaggedItemCount) {
        this.flaggedItemCount = flaggedItemCount;
    }

    public String getDataRuleName() {
        return dataRuleName;
    }

    public void setDataRuleName(String dataRuleName) {
        this.dataRuleName = dataRuleName;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataRuleName == null) ? 0 : dataRuleName.hashCode());
        result = prime * result + ((modelId == null) ? 0 : modelId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BaseRuleResult other = (BaseRuleResult) obj;
        if (dataRuleName == null) {
            if (other.dataRuleName != null)
                return false;
        } else if (!dataRuleName.equals(other.dataRuleName))
            return false;
        if (modelId == null) {
            if (other.modelId != null)
                return false;
        } else if (!modelId.equals(other.modelId))
            return false;
        return true;
    }

}
