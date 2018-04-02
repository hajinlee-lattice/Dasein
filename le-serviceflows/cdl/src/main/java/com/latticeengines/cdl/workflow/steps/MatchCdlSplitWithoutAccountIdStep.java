package com.latticeengines.cdl.workflow.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlSplitParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlSplitConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("matchCdlSplitWithoutAccountIdStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchCdlSplitWithoutAccountIdStep extends RunDataFlow<MatchCdlSplitConfiguration> {

    private static Logger log = LoggerFactory.getLogger(MatchCdlSplitWithoutAccountIdStep.class);

    @Override
    public void onConfigurationInitialized() {
        MatchCdlSplitConfiguration configuration = getConfiguration();
        String targetTableName = NamingUtils.timestamp("MatchCdlSplitWithoutAccontIdTable");
        configuration.setTargetTableName(targetTableName);
        log.info("Target table name: " + targetTableName);
        configuration.setDataFlowParams(createDataFlowParameters());
    }

    private DataFlowParameters createDataFlowParameters() {
        Table inputTable = getInputTable();
        MatchCdlSplitParameters parameters = new MatchCdlSplitParameters(inputTable.getName());
        parameters.expression = InterfaceName.LatticeAccountId.name() + " == null";
        parameters.filterField = InterfaceName.LatticeAccountId.name();
        parameters.retainFields = getListObjectFromContext(CUSTOM_EVENT_MATCH_ATTRIBUTES, String.class);
        return parameters;
    }

    private Table getInputTable() {
        return getObjectFromContext(CUSTOM_EVENT_MATCH_ACCOUNT, Table.class);
    }

    @Override
    public void onExecutionCompleted() {
        Table targetTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID, targetTable);
    }

}
