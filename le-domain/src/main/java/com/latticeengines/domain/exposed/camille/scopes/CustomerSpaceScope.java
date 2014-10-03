package com.latticeengines.domain.exposed.camille.scopes;

public class CustomerSpaceScope extends ConfigurationScope {
    private String contractID;
    private String tenantID;
    private String spaceID;
    
    public CustomerSpaceScope(String contractID, String tenantID, String spaceID) {
        this.contractID = contractID;
        this.tenantID = tenantID;
        this.spaceID = spaceID;
    }
    
    public CustomerSpaceScope(String contractID, String tenantID) {
        this.contractID = contractID;
        this.tenantID = tenantID;
        this.spaceID = null;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contractID == null) ? 0 : contractID.hashCode());
        result = prime * result + ((spaceID == null) ? 0 : spaceID.hashCode());
        result = prime * result + ((tenantID == null) ? 0 : tenantID.hashCode());
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
        CustomerSpaceScope other = (CustomerSpaceScope) obj;
        if (contractID == null) {
            if (other.contractID != null)
                return false;
        } else if (!contractID.equals(other.contractID))
            return false;
        if (spaceID == null) {
            if (other.spaceID != null)
                return false;
        } else if (!spaceID.equals(other.spaceID))
            return false;
        if (tenantID == null) {
            if (other.tenantID != null)
                return false;
        } else if (!tenantID.equals(other.tenantID))
            return false;
        return true;
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
    public String getSpaceID() {
        return spaceID;
    }
    public void setSpaceID(String spaceID) {
        this.spaceID = spaceID;
    }
    
    
}
