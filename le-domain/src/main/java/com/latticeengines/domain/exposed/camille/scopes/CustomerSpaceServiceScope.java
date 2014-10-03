package com.latticeengines.domain.exposed.camille.scopes;

public class CustomerSpaceServiceScope extends ConfigurationScope {
    private String contractID;
    private String tenantID;
    private String spaceID;
    private String serviceName;
    private int dataVersion;
    
    public CustomerSpaceServiceScope(String contractID, String tenantID, String spaceID, String serviceName, int dataVersion) {
        this.contractID = contractID;
        this.tenantID = tenantID;
        this.spaceID = spaceID;
        this.serviceName = serviceName;
        this.dataVersion = dataVersion;
    }
    
    public CustomerSpaceServiceScope(String contractID, String tenantID, String serviceName, int dataVersion) {
        this.contractID = contractID;
        this.tenantID = tenantID;
        this.spaceID = null;
        this.serviceName = serviceName;
        this.dataVersion = dataVersion;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contractID == null) ? 0 : contractID.hashCode());
        result = prime * result + dataVersion;
        result = prime * result + ((serviceName == null) ? 0 : serviceName.hashCode());
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
        CustomerSpaceServiceScope other = (CustomerSpaceServiceScope) obj;
        if (contractID == null) {
            if (other.contractID != null)
                return false;
        } else if (!contractID.equals(other.contractID))
            return false;
        if (dataVersion != other.dataVersion)
            return false;
        if (serviceName == null) {
            if (other.serviceName != null)
                return false;
        } else if (!serviceName.equals(other.serviceName))
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
    
    public void setContractID(String contractiD) {
        this.contractID = contractiD;
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

}
