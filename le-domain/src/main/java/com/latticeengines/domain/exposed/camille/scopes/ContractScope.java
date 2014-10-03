package com.latticeengines.domain.exposed.camille.scopes;

public class ContractScope extends ConfigurationScope {
    private String contractID;

    public ContractScope(String contractID) {
        this.contractID = contractID;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contractID == null) ? 0 : contractID.hashCode());
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
        ContractScope other = (ContractScope) obj;
        if (contractID == null) {
            if (other.contractID != null)
                return false;
        } else if (!contractID.equals(other.contractID))
            return false;
        return true;
    }

    public String getContractID() {
        return contractID;
    }

    public void setContractID(String contractID) {
        this.contractID = contractID;
    }
    
}
