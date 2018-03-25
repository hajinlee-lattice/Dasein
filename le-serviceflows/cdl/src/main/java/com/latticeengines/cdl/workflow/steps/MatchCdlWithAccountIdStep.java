package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlAccountParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlAccountConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("matchCdlWithAccountIdStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchCdlWithAccountIdStep extends RunDataFlow<MatchCdlAccountConfiguration> {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private static Logger log = LoggerFactory.getLogger(MatchCdlWithAccountIdStep.class);

    @Override
    public void onConfigurationInitialized() {
        MatchCdlAccountConfiguration configuration = getConfiguration();
        String targetTableName = configuration.getTargetTableName();
        if (StringUtils.isEmpty(targetTableName)) {
            targetTableName = NamingUtils.timestamp("MatchCdlWithAccontIdTable");
            configuration.setTargetTableName(targetTableName);
        }
        log.info("Target table name: " + targetTableName);
        configuration.setDataFlowParams(createDataFlowParameters());
    }

    private DataFlowParameters createDataFlowParameters() {
        Table inputTable = getInputTable();
        Table accountTable = getAccountTable();
        MatchCdlAccountParameters parameters = new MatchCdlAccountParameters(inputTable.getName(),
                accountTable.getName());
        parameters.setInputMatchFields(Arrays.asList(configuration.getMatchAccountIdColumn()));
        parameters.setAccountMatchFields(Arrays.asList(InterfaceName.AccountId.name()));
        parameters.setDedupe(false);
        return parameters;
    }

    private Table getAccountTable() {
        Table accountTable = dataCollectionProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                TableRoleInCollection.ConsolidatedAccount);
        if (accountTable == null) {
            throw new RuntimeException("There's no Account table!");
        }
        return accountTable;
    }

    private Table getInputTable() {
        Table inputTable = getObjectFromContext(CUSTOM_EVENT_IMPORT, Table.class);
        if (inputTable != null)
            return inputTable;
        return metadataProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                configuration.getMatchInputTableName());
    }

    @Override
    public void onExecutionCompleted() {
        Table targetTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(CUSTOM_EVENT_MATCH_ACCOUNT, targetTable);
        putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, targetTable);
    }

}
