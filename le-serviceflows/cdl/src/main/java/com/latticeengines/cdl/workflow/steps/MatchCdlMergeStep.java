package com.latticeengines.cdl.workflow.steps;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.MatchCdlWithAccountIdWorkflow;
import com.latticeengines.cdl.workflow.MatchCdlWithoutAccountIdWorkflow;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.MatchCdlAccountWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlMergeParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlMergeConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("matchCdlMergeStep")
public class MatchCdlMergeStep extends RunDataFlow<MatchCdlMergeConfiguration> {

    private static Logger log = LoggerFactory.getLogger(MatchCdlMergeStep.class);
    private Table tableWithAccountId;
    private Table tableWithoutAccountId;

    @Inject
    private MatchCdlWithAccountIdWorkflow accountIdWorkflow;
    @Inject
    private MatchCdlWithoutAccountIdWorkflow noAccountIdWorkflow;

    @Override
    public void onConfigurationInitialized() {
        MatchCdlMergeConfiguration configuration = getConfiguration();
        String targetTableName = NamingUtils.timestampWithRandom("MatchCdlMergeTable");
        configuration.setTargetTableName(targetTableName);
        log.info("Target table name: " + targetTableName);
        configuration.setDataFlowParams(createDataFlowParameters());
    }

    private DataFlowParameters createDataFlowParameters() {
        tableWithAccountId = getObjectFromContext(CUSTOM_EVENT_MATCH_ACCOUNT_ID, Table.class);
        tableWithoutAccountId = getObjectFromContext(CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID, Table.class);

        MatchCdlMergeParameters parameters = new MatchCdlMergeParameters(
                tableWithAccountId != null ? tableWithAccountId.getName() : null,
                tableWithoutAccountId != null ? tableWithoutAccountId.getName() : null);
        return parameters;
    }

    @Override
    public void onExecutionCompleted() {
        Table targetTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(EVENT_TABLE, targetTable);

        enableEmbeddedWorkflow(getParentNamespace(), accountIdWorkflow.name(),
                MatchCdlAccountWorkflowConfiguration.class);
        enableEmbeddedWorkflow(getParentNamespace(), noAccountIdWorkflow.name(),
                MatchCdlAccountWorkflowConfiguration.class);

        if (tableWithAccountId != null) {
            metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), tableWithAccountId.getName());
        }
        if (tableWithoutAccountId != null) {
            metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), tableWithoutAccountId.getName());
        }

        removeObjectFromContext(CUSTOM_EVENT_MATCH_ACCOUNT_ID);
        removeObjectFromContext(CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID);

    }

}
