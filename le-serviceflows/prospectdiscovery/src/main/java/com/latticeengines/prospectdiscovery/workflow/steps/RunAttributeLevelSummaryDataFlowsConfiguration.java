package com.latticeengines.prospectdiscovery.workflow.steps;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;


public class RunAttributeLevelSummaryDataFlowsConfiguration extends MicroserviceStepConfiguration {

    private String eventColumnName;
    
    private String eventTableName;
    
    private List<String> attributes = new ArrayList<>();
    
    private Double avgProbability;
    
    private TargetMarket targetMarket;
    
    private String internalResourceHostPort;
    
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

    public Double getAvgProbability() {
        return avgProbability;
    }

    public void setAvgProbability(Double avgProbability) {
        this.avgProbability = avgProbability;
    }

    public TargetMarket getTargetMarket() {
        return targetMarket;
    }

    public void setTargetMarket(TargetMarket targetMarket) {
        this.targetMarket = targetMarket;
    }

    public String getInternalResourceHostPort() {
        return internalResourceHostPort;
    }

    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }

}
