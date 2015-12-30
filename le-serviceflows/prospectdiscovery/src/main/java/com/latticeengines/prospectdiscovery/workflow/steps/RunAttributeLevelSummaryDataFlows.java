package com.latticeengines.prospectdiscovery.workflow.steps;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.flows.CreateAttributeLevelSummaryParameters;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("runAttributeLevelSummaryDataFlows")
public class RunAttributeLevelSummaryDataFlows extends BaseWorkflowStep<RunAttributeLevelSummaryDataFlowsConfiguration> {
    
    private static final Log log = LogFactory.getLog(RunAttributeLevelSummaryDataFlows.class);

    private String getEventTable() {
        return executionContext.getString(EVENT_TABLE);
    }

    @Override
    public void execute() {
        log.info("Inside RunAttributeLevelSummaryDataFlows execute()");
        
        RunAttributeLevelSummaryDataFlow runAttributeLevelSummaryDataFlow = new RunAttributeLevelSummaryDataFlow();
        RunAttributeLevelSummaryDataFlowConfiguration dataFlowConfig = new RunAttributeLevelSummaryDataFlowConfiguration();
        dataFlowConfig.setMicroServiceHostPort(configuration.getMicroServiceHostPort());
        dataFlowConfig.setCustomerSpace(configuration.getCustomerSpace());

        runAttributeLevelSummaryDataFlow.setConfiguration(dataFlowConfig);
        runAttributeLevelSummaryDataFlow.setup();
        
        AttrLevelParameters attrLevelParams = getParameters();

        for (String attribute : configuration.getAttributes()) {
            String[] attrs = null;
            if (attrLevelParams.aggregationType.equals("COUNT")) {
                attrs = new String[] { attribute, getEventColumnName() };
            } else {
                attrs = new String[] { attribute };
            }
            List<String> groupByCols = Arrays.asList(attrs);
            
            CreateAttributeLevelSummaryParameters params = new CreateAttributeLevelSummaryParameters(getEventTable(), groupByCols, attrLevelParams.aggregateColumn);
            params.aggregationType = attrLevelParams.aggregationType;
            
            runAttributeLevelSummaryDataFlow.getConfiguration().setDataFlowParams(params);
            String name = "CreateAttributeLevelSummary_" + attribute + attrLevelParams.suffix;
            runAttributeLevelSummaryDataFlow.getConfiguration().setName(name);
            runAttributeLevelSummaryDataFlow.getConfiguration().setTargetPath("/" + name);
            runAttributeLevelSummaryDataFlow.execute();
        }
        executionContext.putString(ATTR_LEVEL_TYPE, "COUNT");
        executionContext.putString(EVENT_TABLE, getMatchTable());
    }
    
    private String getEventColumnName() {
        String eventColumnName = getStringValueFromContext(EVENT_COLUMN);
        if (eventColumnName == null) {
            eventColumnName = configuration.getEventColumnName();
        }
        return eventColumnName;
    }
    
    private String getMatchTable() {
        String matchTableName = getStringValueFromContext(MATCH_TABLE);
        if (matchTableName == null) {
            matchTableName = configuration.getEventTableName();
        }
        return matchTableName;
    }
    
    private AttrLevelParameters getParameters() {
        AttrLevelParameters params = new AttrLevelParameters();
        String attrLevelType = getStringValueFromContext(ATTR_LEVEL_TYPE);
        if (attrLevelType.equals("COUNT")) {
            params.aggregateColumn = "Id";
            params.aggregationType = "COUNT";
            params.suffix = "";
        } else {
            params.aggregateColumn = "Probability";
            params.aggregationType = "AVG";
            params.suffix = "_Probability";
        }
        return params;
    }
    
    static class AttrLevelParameters {
        String suffix;
        String aggregateColumn;
        String aggregationType;
    }
    
}
