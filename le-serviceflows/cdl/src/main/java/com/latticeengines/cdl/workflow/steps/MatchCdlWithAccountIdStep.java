package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import com.latticeengines.domain.exposed.serviceflows.cdl.CustomEventMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlAccountParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlAccountConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlSplitConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("matchCdlWithAccountIdStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchCdlWithAccountIdStep extends RunDataFlow<MatchCdlAccountConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MatchCdlWithAccountIdStep.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private boolean hasCustomerAccountId = true;
    private boolean renameIdColumn = false;

    @Override
    public void onConfigurationInitialized() {
        MatchCdlAccountConfiguration configuration = getConfiguration();
        String targetTableName = configuration.getTargetTableName();
        if (StringUtils.isEmpty(targetTableName)) {
            targetTableName = NamingUtils.timestampWithRandom("MatchCdlWithAccontIdTable");
            configuration.setTargetTableName(targetTableName);
        }
        log.info("Target table name: " + targetTableName);
        configuration.setDataFlowParams(createDataFlowParameters());
    }

    private DataFlowParameters createDataFlowParameters() {
        Table inputTable = getInputTable();
        String[] attributeNames = inputTable.getAttributeNames();
        List<String> inputAttributeList = new ArrayList<>(Arrays.asList(attributeNames));
        Table accountTable = getAccountTable();
        MatchCdlAccountParameters parameters = new MatchCdlAccountParameters(inputTable.getName(),
                accountTable.getName());
        parameters.setInputMatchFields(Arrays.asList(configuration.getMatchAccountIdColumn()));
        String customerAccountId = InterfaceName.AccountId.name();
        if (getConfiguration().isEntityMatchEnabled()) {
            if (getConfiguration().isMapToLatticeAccount()
                    && inputAttributeList.contains(configuration.getMatchAccountIdColumn())) {
                parameters.setRenameIdOnly(true);
                renameIdColumn = true;
                customerAccountId = InterfaceName.CustomerAccountId.name();
                inputAttributeList.remove(configuration.getMatchAccountIdColumn());
                inputAttributeList.add(0, InterfaceName.CustomerAccountId.name());
            }
            hasCustomerAccountId = false;
        }
        parameters.setAccountMatchFields(Arrays.asList(customerAccountId));
        parameters.setHasAccountId(true);

        List<String> accountAttributeList = Arrays.asList(accountTable.getAttributeNames());
        List<String> inputSkippedAttributeList = new ArrayList<>(inputAttributeList);
        inputSkippedAttributeList.removeAll(accountAttributeList);
        putObjectInContext(CUSTOM_EVENT_MATCH_ATTRIBUTES, inputAttributeList);
        putObjectInContext(INPUT_SKIPPED_ATTRIBUTES_KEY, inputSkippedAttributeList);
        return parameters;
    }

    @Override
    public void execute() {
        if (hasCustomerAccountId || renameIdColumn) {
            super.execute();
        }
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
        if (hasCustomerAccountId) {
            Table targetTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                    configuration.getTargetTableName());
            putObjectInContext(CUSTOM_EVENT_MATCH_ACCOUNT, targetTable);
            putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, targetTable);
        } else {
            Table targetTable = renameIdColumn
                    ? metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                            configuration.getTargetTableName())
                    : getInputTable();
            putObjectInContext(CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID, targetTable);
            String ns = getParentNamespace();
            ns = ns.lastIndexOf(".") == -1 ? "" : ns.substring(0, ns.lastIndexOf("."));
            skipEmbeddedWorkflowSteps(ns, "customEventMatchWorkflow", CustomEventMatchWorkflowConfiguration.class,
                    Arrays.asList(MatchCdlSplitConfiguration.class.getSimpleName()));
        }
    }

}
