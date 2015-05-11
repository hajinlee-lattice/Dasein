package com.latticeengines.domain.exposed.admin;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateVisiDBDLRequest {

    private String tenantName;
    private String tenantAlias;
    private String dmDeployment;
    private String contractExternalID;
    private String ownerEmail;
    private boolean createNewVisiDB;
    private boolean caseSensitive;
    private String visiDBName;
    private String visiDBLocation;
    private String visiDBFileDirectory;
    private int cacheLimit;
    private int diskspaceLimit;
    private int permanentStoreOption;
    private String permanentStorePath;

    private CreateVisiDBDLRequest(Builder builder) {
        this.tenantName = builder.tenantName;
        this.dmDeployment = builder.dmDeployment;
        this.contractExternalID = builder.contractExternalID;
        this.tenantAlias = builder.tenantAlias;
        this.ownerEmail = builder.ownerEmail;
        this.createNewVisiDB = builder.createNewVisiDB;
        this.caseSensitive = builder.caseSensitive;
        this.visiDBName = builder.visiDBName;
        this.visiDBLocation = builder.visiDBLocation;
        this.visiDBFileDirectory = builder.visiDBFileDirectory;
        this.cacheLimit = builder.cacheLimit;
        this.diskspaceLimit = builder.diskspaceLimit;
        this.permanentStoreOption = builder.permanentStoreOption;
        this.permanentStorePath = builder.permanentStorePath;
    }

    @JsonProperty("tenantName")
    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    @JsonProperty("tenantAlias")
    public String getTenantAlias() {
        return tenantAlias;
    }

    public void setTenantAlias(String tenantAlias) {
        this.tenantAlias = tenantAlias;
    }

    @JsonProperty("ownerEmail")
    public String getOwnerEmail() {
        return ownerEmail;
    }

    public void setOwnerEmail(String ownerEmail) {
        this.ownerEmail = ownerEmail;
    }

    @JsonProperty("dmDeployment")
    public String getDMDeployment() {
        return dmDeployment;
    }

    public void setDMDeployment(String dmDeployment) {
        this.dmDeployment = dmDeployment;
    }

    @JsonProperty("contractExternalID")
    public String getContractExternalID() {
        return contractExternalID;
    }

    public void setContractExternalID(String contractExternalID) {
        this.contractExternalID = contractExternalID;
    }

    @JsonProperty("caseSensitive")
    public boolean getCaseSensitive() {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    @JsonProperty("createNewVisiDB")
    public boolean getCreateNewVisiDB() {
        return createNewVisiDB;
    }

    public void setCreateNewVisiDB(boolean createNewVisiDB) {
        this.createNewVisiDB = createNewVisiDB;
    }

    @JsonProperty("visiDBName")
    public String getVisiDBName() {
        return visiDBName;
    }

    public void setVisiDBName(String visiDBName) {
        this.visiDBName = visiDBName;
    }

    @JsonProperty("visiDBLocation")
    public String getVisiDBLocation() {
        return visiDBLocation;
    }

    public void setVisiDBLocation(String visiDBLocation) {
        this.visiDBLocation = visiDBLocation;
    }

    @JsonProperty("visiDBFileDirectory")
    public String getVisiDBFileDirectory() {
        return visiDBFileDirectory;
    }

    public void setVisiDBFileDirectory(String visiDBFileDirectory) {
        this.visiDBFileDirectory = visiDBFileDirectory;
    }

    @JsonProperty("cacheLimit")
    public int getCacheLimit() {
        return cacheLimit;
    }

    public void setCacheLimit(int cacheLimit) {
        this.cacheLimit = cacheLimit;
    }

    @JsonProperty("diskspaceLimit")
    public int getDiskspaceLimit() {
        return diskspaceLimit;
    }

    public void setDiskspaceLimit(int diskspaceLimit) {
        this.diskspaceLimit = diskspaceLimit;
    }

    @JsonProperty("permanentStoreOption")
    public int getPermanentStoreOption() {
        return permanentStoreOption;
    }

    public void setPermanentStoreOption(int permanentStoreOption) {
        this.permanentStoreOption = permanentStoreOption;
    }

    @JsonProperty("permanentStorePath")
    public String getPermanentStorePath() {
        return permanentStorePath;
    }

    public void setPermanentStorePath(String permanentStorePath) {
        this.permanentStorePath = permanentStorePath;
    }

    public static class Builder {
        private String tenantName;
        private String tenantAlias;
        private String dmDeployment;
        private String contractExternalID;
        private String ownerEmail;
        private boolean createNewVisiDB;
        private boolean caseSensitive;
        private String visiDBName;
        private String visiDBLocation;
        private String visiDBFileDirectory;
        private int cacheLimit;
        private int diskspaceLimit;
        private int permanentStoreOption;
        private String permanentStorePath;

        public Builder(String tenantName, String dmDeployment, String contractExternalID) {
            this.tenantName = tenantName;
            this.dmDeployment = dmDeployment;
            this.contractExternalID = contractExternalID;
        }

        public Builder tenantName(String tenantName) {
            this.tenantName = tenantName;
            return this;
        }

        public Builder dmDeployment(String dmDeployment) {
            this.dmDeployment = dmDeployment;
            return this;
        }

        public Builder contractExternalID(String contractExternalID) {
            this.contractExternalID = contractExternalID;
            return this;
        }

        public Builder tenantAlias(String tenantAlias) {
            this.tenantAlias = tenantAlias;
            return this;
        }

        public Builder ownerEmail(String ownerEmail) {
            this.ownerEmail = ownerEmail;
            return this;
        }

        public Builder visiDBName(String visiDBName) {
            this.visiDBName = visiDBName;
            return this;
        }

        public Builder visiDBLocation(String visiDBLocation) {
            this.visiDBLocation = visiDBLocation;
            return this;
        }

        public Builder visiDBFileDirectory(String visiDBFileDirectory) {
            this.visiDBFileDirectory = visiDBFileDirectory;
            return this;
        }

        public Builder caseSensitive(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
            return this;
        }

        public Builder createNewVisiDB(boolean createNewVisiDB) {
            this.createNewVisiDB = createNewVisiDB;
            return this;
        }

        public Builder cacheLimit(int cacheLimit) {
            this.cacheLimit = cacheLimit;
            return this;
        }

        public Builder diskspaceLimit(int diskspaceLimit) {
            this.diskspaceLimit = diskspaceLimit;
            return this;
        }

        public Builder permanentStoreOption(int permanentStoreOption) {
            this.permanentStoreOption = permanentStoreOption;
            return this;
        }

        public Builder permanentStorePath(String permanentStorePath) {
            this.permanentStorePath = permanentStorePath;
            return this;
        }

        public CreateVisiDBDLRequest build() {
            return new CreateVisiDBDLRequest(this);
        }
    }
}
