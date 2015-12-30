package com.latticeengines.prospectdiscovery.workflow.steps;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;


public class RunAttributeLevelSummaryDataFlowsConfiguration extends MicroserviceStepConfiguration {

    private String eventColumnName;
    
    private String eventTableName;
    
    private List<String> attributes = new ArrayList<>();
    
    public String getEventColumnName() {
        return eventColumnName;
    }
    
    public void setEventColumnName(String eventColumnName) {
        this.eventColumnName = eventColumnName;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    public String getEventTableName() {
        return eventTableName;
    }

    public void setEventTableName(String eventTableName) {
        this.eventTableName = eventTableName;
    }

}
