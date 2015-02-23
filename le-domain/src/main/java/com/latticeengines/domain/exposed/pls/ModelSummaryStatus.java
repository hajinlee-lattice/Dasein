package com.latticeengines.domain.exposed.pls;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonValue;


public enum ModelSummaryStatus {
    
    // new status has to be at the end
    ACTIVE(0), INACTIVE(1), DELETED(2);
    
    private ModelSummaryStatus(int statusId) {
        this.statusId = statusId;
    }
    private int statusId;
    
    public int getStatusId() {
        return statusId;
    }
    
    @JsonValue
    public String getName() {
        return StringUtils.capitalize(super.name().toLowerCase());
    }
}
