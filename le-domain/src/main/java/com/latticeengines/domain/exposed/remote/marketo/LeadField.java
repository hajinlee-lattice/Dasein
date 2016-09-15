package com.latticeengines.domain.exposed.remote.marketo;

public class LeadField {

    private String apiName;
    private String displayName;
    private String dataType;

    public String getApiName() {
        return apiName;
    }
    public void setApiName(String name) {
        this.apiName = name;
    }
    public String getDisplayName() {
        return displayName;
    }
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }
    public String getDataType() {
        return dataType;
    }
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
}
