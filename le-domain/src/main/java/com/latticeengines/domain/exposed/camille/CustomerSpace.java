package com.latticeengines.domain.exposed.camille;

public class CustomerSpace {
    private String contractID;
    private String tenantID;
    private String spaceID;
    
    /**
     * Uses the default space.
     */
    public CustomerSpace(String contractID, String tenantID) {
        this.setContractID(contractID);
        this.setTenantID(tenantID);
        this.setSpaceID(null);
    }
    
    /**
     * Uses the specified space.
     */
    public CustomerSpace(String contractID, String tenantID, String spaceID) {
        this.setContractID(contractID);
        this.setTenantID(tenantID);
        this.setSpaceID(spaceID);
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
    
    public void setUseDefaultSpace() {
        this.spaceID = null;
    }
    
    public boolean getUseDefaultSpace() {
        return this.spaceID == null;
    }
}
