package com.latticeengines.domain.exposed.camille.scopes;

public class TenantScope extends ConfigurationScope {
    private String contractId;
    private String tenantId;
    
    public TenantScope(String contractId, String tenantId) {
        this.contractId = contractId;
        this.tenantId = tenantId;
    }
    
    public String getContractId() {
        return contractId;
    }
    
    public void setContractId(String contractId) {
        this.contractId = contractId;
    }
    
    public String getTenantId() {
        return tenantId;
    }
    
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contractId == null) ? 0 : contractId.hashCode());
        result = prime * result + ((tenantId == null) ? 0 : tenantId.hashCode());
        result = prime * result + getType().hashCode();
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
        TenantScope other = (TenantScope) obj;
        if (contractId == null) {
            if (other.contractId != null)
                return false;
        } else if (!contractId.equals(other.contractId))
            return false;
        if (tenantId == null) {
            if (other.tenantId != null)
                return false;
        } else if (!tenantId.equals(other.tenantId))
            return false;
        return true;
    }

    @Override
    public Type getType() {
        return Type.TENANT;
    }
    
    
}
