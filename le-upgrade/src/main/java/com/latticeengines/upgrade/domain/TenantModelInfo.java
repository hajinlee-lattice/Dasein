package com.latticeengines.upgrade.domain;

public class TenantModelInfo {

    private String tenant;

    private String modelGuid;

    public String getTenant(){
        return this.tenant;
    }
    
    public void setTenant(String tenant){
        this.tenant = tenant;
    }

    public String getModelGuid(){
        return this.modelGuid;
    }

    public void setModelGuid(String modelGuid){
        this.modelGuid = modelGuid;
    }

}
