package com.latticeengines.domain.exposed.camille.scopes;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class CustomerSpaceScope extends ConfigurationScope {
    private String contractId;
    private String tenantId;
    private String spaceId;

    public CustomerSpaceScope(String contractId, String tenantId, String spaceId) {
        this.contractId = contractId;
        this.tenantId = tenantId;
        this.spaceId = spaceId;
    }

    public CustomerSpaceScope(CustomerSpace customerSpace) {
        this.contractId = customerSpace.getContractId();
        this.tenantId = customerSpace.getTenantId();
        this.spaceId = customerSpace.getSpaceId();
    }

    public CustomerSpace getCustomerSpace() {
        return new CustomerSpace(contractId, tenantId, spaceId);
    }

    @Override
    public String toString() {
        return getCustomerSpace().toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contractId == null) ? 0 : contractId.hashCode());
        result = prime * result + ((spaceId == null) ? 0 : spaceId.hashCode());
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
        CustomerSpaceScope other = (CustomerSpaceScope) obj;
        if (contractId == null) {
            if (other.contractId != null)
                return false;
        } else if (!contractId.equals(other.contractId))
            return false;
        if (spaceId == null) {
            if (other.spaceId != null)
                return false;
        } else if (!spaceId.equals(other.spaceId))
            return false;
        if (tenantId == null) {
            if (other.tenantId != null)
                return false;
        } else if (!tenantId.equals(other.tenantId))
            return false;
        return true;
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

    public String getSpaceId() {
        return spaceId;
    }

    public void setSpaceId(String spaceId) {
        this.spaceId = spaceId;
    }

    @Override
    public Type getType() {
        return Type.CUSTOMER_SPACE;
    }

}
