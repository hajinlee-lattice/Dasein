package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class CrmConfig {

    private CrmCredential crmCredential;
    
    private String dataProviderName;
    private String entityType;
    private String version;

    @JsonProperty("CrmCredential")
    public CrmCredential getCrmCredential() {
        return crmCredential;
    }

    @JsonProperty("CrmCredential")
    public void setCrmCredential(CrmCredential crmCredential) {
        this.crmCredential = crmCredential;
    }

    @JsonProperty("EntityType")
    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    @JsonProperty("EntityType")
    public String getEntityType() {
        return entityType;
    }

    @JsonProperty("DataProviderName")
    public String getDataProviderName() {
        return dataProviderName;
    }

    @JsonProperty("DataProviderName")
    public void setDataProviderName(String dataProviderName) {
        this.dataProviderName = dataProviderName;
    }

    @JsonProperty("Version")
    public String getVersion() {
        return this.version;
    }

    @JsonProperty("Version")
    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
