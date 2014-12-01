package com.latticeengines.domain.exposed.camille.scopes;

public class ContractScope extends ConfigurationScope {
    private String contractId;

    public ContractScope(String contractId) {
        this.contractId = contractId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contractId == null) ? 0 : contractId.hashCode());
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
        ContractScope other = (ContractScope) obj;
        if (contractId == null) {
            if (other.contractId != null)
                return false;
        } else if (!contractId.equals(other.contractId))
            return false;
        return true;
    }

    public String getContractId() {
        return contractId;
    }

    public void setContractID(String contractId) {
        this.contractId = contractId;
    }

    @Override
    public Type getType() {
        return Type.CONTRACT;
    }

}
