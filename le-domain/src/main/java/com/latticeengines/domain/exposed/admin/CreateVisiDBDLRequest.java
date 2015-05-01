package com.latticeengines.domain.exposed.admin;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateVisiDBDLRequest {

    private String tenantName;
    private String tenantAlias;
    private String ownerEmail;
    private String dmDeployment;
    private String contractExternalID;
    private boolean createNewVisiDB;
    private String visiDBName;
    private String visiDBLocation;
    
    public CreateVisiDBDLRequest(String tenantName, String tenantAlias, String ownerEmail, String dmDeployment, String contractExternalID, boolean createNewVisiDB, String visiDBName, String visiDBLocation){
        this.tenantName = tenantName;
        this.tenantAlias = tenantAlias;
        this.ownerEmail = ownerEmail;
        this.dmDeployment = dmDeployment;
        this.contractExternalID = contractExternalID;
        this.createNewVisiDB = createNewVisiDB;
        this.visiDBName = visiDBName;
        this.visiDBLocation = visiDBLocation;
    }

    @JsonProperty("tenantName")
    public String getTenantName() {
        return tenantName;
    }

    @JsonProperty("tenantName")
    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }
    
    @JsonProperty("tenantAlias")
    public String getTenantAlias() {
        return tenantAlias;
    }

    @JsonProperty("tenantAlias")
    public void setTenantAlias(String tenantAlias) {
        this.tenantAlias = tenantAlias;
    }
    
    @JsonProperty("ownerEmail")
    public String getOwnerEmail() {
        return ownerEmail;
    }

    @JsonProperty("ownerEmail")
    public void setOwnerEmail(String ownerEmail) {
        this.ownerEmail = ownerEmail;
    }
    
    @JsonProperty("dmDeployment")
    public String getDMDeployment() {
        return dmDeployment;
    }

    @JsonProperty("dmDeployment")
    public void setDMDeployment(String dmDeployment) {
        this.dmDeployment = dmDeployment;
    }
    @JsonProperty("contractExternalID")
    public String getContractExternalID() {
        return contractExternalID;
    }

    @JsonProperty("contractExternalID")
    public void setContractExternalID(String contractExternalID) {
        this.contractExternalID = contractExternalID;
    }
    @JsonProperty("createNewVisiDB")
    public boolean getCreateNewVisiDB() {
        return createNewVisiDB;
    }

    @JsonProperty("createNewVisiDB")
    public void setCreateNewVisiDB(boolean createNewVisiDB) {
        this.createNewVisiDB = createNewVisiDB;
    }
    @JsonProperty("visiDBName")
    public String getVisiDBName() {
        return visiDBName;
    }

    @JsonProperty("visiDBName")
    public void setVisiDBName(String visiDBName) {
        this.visiDBName = visiDBName;
    }
    @JsonProperty("visiDBLocation")
    public String getVisiDBLocation() {
        return visiDBLocation;
    }

    @JsonProperty("visiDBLocation")
    public void setvisiDBLocation(String visiDBLocation) {
        this.visiDBLocation = visiDBLocation;
    }
}
