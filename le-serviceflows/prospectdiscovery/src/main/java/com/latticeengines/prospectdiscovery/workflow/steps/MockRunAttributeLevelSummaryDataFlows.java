package com.latticeengines.prospectdiscovery.workflow.steps;

import com.latticeengines.domain.exposed.dataflow.flows.CreateAttributeLevelSummaryParameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component("mockRunAttributeLevelSummaryDataFlows")
public class MockRunAttributeLevelSummaryDataFlows
    extends CreateSummaryWorkflow<RunAttributeLevelSummaryDataFlowsConfiguration> {

    private static final Log log = LogFactory.getLog(MockRunAttributeLevelSummaryDataFlows.class);

    @Override
    public void execute() {
        log.info("Inside RunAttributeLevelSummaryDataFlows execute()");

        RunAttributeLevelSummaryDataFlow runAttributeLevelSummaryDataFlow = getRunAttributeLevelSummaryDataFlow();
        RegisterAttributeLevelSummaryReport registerAttributeLevelSummaryReport = getRegisterAttributeLevelSummaryReport();

        AttrLevelParameters attrLevelParams = getParameters();

        String attribute = configuration.getAttributes().get(0);
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
        runAttributeLevelSummaryDataFlow.getConfiguration().setName(name);
        runAttributeLevelSummaryDataFlow.getConfiguration().setTargetPath("/" + name);
        runAttributeLevelSummaryDataFlow.execute();

        registerAttributeLevelSummaryReport.execute(name, createReportParams(params.aggregationType, attrs));

        executionContext.putString(ATTR_LEVEL_TYPE, "COUNT");
        executionContext.putString(EVENT_TABLE, getMatchTable());
    }

}
