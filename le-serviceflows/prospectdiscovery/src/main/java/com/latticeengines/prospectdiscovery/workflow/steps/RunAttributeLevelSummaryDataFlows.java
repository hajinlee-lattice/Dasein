package com.latticeengines.prospectdiscovery.workflow.steps;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.RunAttributeLevelSummaryDataFlowsConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.flows.CreateAttributeLevelSummaryParameters;

@Component("runAttributeLevelSummaryDataFlows")
public class RunAttributeLevelSummaryDataFlows extends
        CreateSummaryWorkflow<RunAttributeLevelSummaryDataFlowsConfiguration> {

    private static final Log log = LogFactory.getLog(RunAttributeLevelSummaryDataFlows.class);

    @Override
    public void execute() {
        log.info("Inside RunAttributeLevelSummaryDataFlows execute()");

        RunAttributeLevelSummaryDataFlow runAttributeLevelSummaryDataFlow = getRunAttributeLevelSummaryDataFlow();
        RegisterAttributeLevelSummaryReport registerAttributeLevelSummaryReport = getRegisterAttributeLevelSummaryReport();

        AttrLevelParameters attrLevelParams = getParameters();

        for (String attribute : configuration.getAttributes()) {
            String[] attrs = null;
            if (attrLevelParams.aggregationType.equals("COUNT")) {
                attrs = new String[] { attribute, getEventColumnName() };
            } else {
                attrs = new String[] { attribute, "AverageProbability" };
            }
            List<String> groupByCols = Arrays.asList(attrs);

            CreateAttributeLevelSummaryParameters params = new CreateAttributeLevelSummaryParameters(
                    getEventTable(), groupByCols, attrLevelParams.aggregateColumn);
            params.aggregationType = attrLevelParams.aggregationType;

            runAttributeLevelSummaryDataFlow.getConfiguration().setDataFlowParams(params);
            String name = "CreateAttributeLevelSummary_" + attribute + attrLevelParams.suffix;
            runAttributeLevelSummaryDataFlow.getConfiguration().setTargetTableName(name);
            runAttributeLevelSummaryDataFlow.execute();
            
            registerAttributeLevelSummaryReport.execute(name, createReportParams(params.aggregationType, attrs));
        }
        putStringValueInContext(ATTR_LEVEL_TYPE, "COUNT");
        putStringValueInContext(EVENT_TABLE, getMatchTable());
    }

}
