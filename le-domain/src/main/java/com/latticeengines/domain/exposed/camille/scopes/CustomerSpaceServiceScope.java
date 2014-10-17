package com.latticeengines.domain.exposed.camille.scopes;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class CustomerSpaceServiceScope extends ConfigurationScope {
    private String contractId;
    private String tenantId;
    private String spaceId;
    private String serviceName;
    private int dataVersion;

    public CustomerSpaceServiceScope(String contractId, String tenantId, String spaceId, String serviceName,
            int dataVersion) {
        this.contractId = contractId;
        this.tenantId = tenantId;
        this.spaceId = spaceId;
        this.serviceName = serviceName;
        this.dataVersion = dataVersion;
    }

    public CustomerSpaceServiceScope(CustomerSpace space, String serviceName, int dataVersion) {
        this.contractId = space.getContractId();
        this.tenantId = space.getTenantId();
        this.spaceId = space.getSpaceId();
        this.serviceName = serviceName;
        this.dataVersion = dataVersion;
    }

    public CustomerSpaceServiceScope(String contractId, String tenantId, String serviceName, int dataVersion) {
        this.contractId = contractId;
        this.tenantId = tenantId;
        this.spaceId = null;
        this.serviceName = serviceName;
        this.dataVersion = dataVersion;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contractId == null) ? 0 : contractId.hashCode());
        result = prime * result + dataVersion;
        result = prime * result + ((serviceName == null) ? 0 : serviceName.hashCode());
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
        CustomerSpaceServiceScope other = (CustomerSpaceServiceScope) obj;
        if (contractId == null) {
            if (other.contractId != null)
                return false;
        } else if (!contractId.equals(other.contractId))
            return false;
        if (dataVersion != other.dataVersion)
            return false;
        if (serviceName == null) {
            if (other.serviceName != null)
                return false;
        } else if (!serviceName.equals(other.serviceName))
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

    public CustomerSpace getCustomerSpace() {
        return new CustomerSpace(contractId, tenantId, spaceId);
    }

    @Override
    public String toString() {
        return String.format("[CustomerSpace=%s, Service=%s, DataVersion=%d]", getCustomerSpace(), serviceName,
                dataVersion);
    }

    public String getContractId() {
        return contractId;
    }

    public void setContractID(String contractId) {
        this.contractId = contractId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantID(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getSpaceId() {
        return spaceId;
    }

    public void setSpaceID(String spaceID) {
        this.spaceId = spaceID;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public int getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(int dataVersion) {
        this.dataVersion = dataVersion;
    }

    @Override
    public Type getType() {
        return Type.CUSTOMER_SPACE_SERVICE;
    }
}
