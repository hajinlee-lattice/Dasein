package com.latticeengines.domain.exposed.camille.scopes;

public class TenantScope extends ConfigurationScope {
    private String contractID;
    private String tenantID;
    
    public TenantScope(String contractID, String tenantID) {
        this.contractID = contractID;
        this.tenantID = tenantID;
    }
    
    public String getContractID() {
        return contractID;
    }
    
    public void setContractID(String contractID) {
        this.contractID = contractID;
    }
    
    public String getTenantID() {
        return tenantID;
    }
    
    public void setTenantID(String tenantID) {
        this.tenantID = tenantID;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contractID == null) ? 0 : contractID.hashCode());
        result = prime * result + ((tenantID == null) ? 0 : tenantID.hashCode());
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
        if (contractID == null) {
            if (other.contractID != null)
                return false;
        } else if (!contractID.equals(other.contractID))
            return false;
        if (tenantID == null) {
            if (other.tenantID != null)
                return false;
        } else if (!tenantID.equals(other.tenantID))
            return false;
        return true;
    }

    @Override
    public Type getType() {
        return Type.TENANT;
    }
    
    
}
