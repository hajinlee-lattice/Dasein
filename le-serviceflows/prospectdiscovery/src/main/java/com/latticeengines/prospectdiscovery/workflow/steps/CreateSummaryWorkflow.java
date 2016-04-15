package com.latticeengines.prospectdiscovery.workflow.steps;

import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class CreateSummaryWorkflow<T extends RunAttributeLevelSummaryDataFlowsConfiguration> extends BaseWorkflowStep<T> {

    @Autowired
    private RunAttributeLevelSummaryDataFlow runAttributeLevelSummaryDataFlow;

    protected String getEventTable() {
        return executionContext.getString(EVENT_TABLE);
    }

    protected Object[] createReportParams(String aggregationType, String[] attrs) {
        if (aggregationType.equals("COUNT")) {
            return new Object[] { attrs[0], attrs[1] };
        }
        return new Object[] { attrs[0] };
    }

    protected RunAttributeLevelSummaryDataFlow getRunAttributeLevelSummaryDataFlow() {
        RunAttributeLevelSummaryDataFlowConfiguration dataFlowConfig = new RunAttributeLevelSummaryDataFlowConfiguration();
        dataFlowConfig.setMicroServiceHostPort(configuration.getMicroServiceHostPort());
        dataFlowConfig.setCustomerSpace(configuration.getCustomerSpace());

        runAttributeLevelSummaryDataFlow.setConfiguration(dataFlowConfig);
        runAttributeLevelSummaryDataFlow.setup();

        return runAttributeLevelSummaryDataFlow;
    }

    protected RegisterAttributeLevelSummaryReport getRegisterAttributeLevelSummaryReport() {
        RegisterAttributeLevelSummaryReport registerAttributeLevelSummaryReport = new RegisterAttributeLevelSummaryReport();
        TargetMarketStepConfiguration targetMarketStepConfig = new TargetMarketStepConfiguration();
        targetMarketStepConfig.setMicroServiceHostPort(configuration.getMicroServiceHostPort());
        targetMarketStepConfig.setCustomerSpace(configuration.getCustomerSpace());
        targetMarketStepConfig.setTargetMarket(configuration.getTargetMarket());
        targetMarketStepConfig.setInternalResourceHostPort(configuration.getInternalResourceHostPort());

        registerAttributeLevelSummaryReport.setConfiguration(targetMarketStepConfig);
        registerAttributeLevelSummaryReport.setup();

        return registerAttributeLevelSummaryReport;
    }

    protected String getEventColumnName() {
        String eventColumnName = getStringValueFromContext(EVENT_COLUMN);
        if (eventColumnName == null) {
            eventColumnName = configuration.getEventColumnName();
        }
        return eventColumnName;
    }

    protected String getMatchTable() {
        String matchTableName = getStringValueFromContext(MATCH_TABLE);
        if (matchTableName == null) {
            matchTableName = configuration.getEventTableName();
        }
        return matchTableName;
    }

    protected AttrLevelParameters getParameters() {
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

    protected static class AttrLevelParameters {
        String suffix;
        String aggregateColumn;
        String aggregationType;
    }

}
