package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CreateCdlEventTableParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("createCdlEventTableStep")
public class CreateCdlEventTableStep extends RunDataFlow<CreateCdlEventTableConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CreateCdlEventTableStep.class);
    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Value("${dataplatform.queue.scheme}")
    private String queueScheme;

    @Override
    public void onConfigurationInitialized() {
        CreateCdlEventTableConfiguration configuration = getConfiguration();
        configuration.setTargetTableName(configuration.getOutputTableName());
        configuration.setApplyTableProperties(true);
        configuration.setDataFlowParams(createDataFlowParameters());
    }

    private DataFlowParameters createDataFlowParameters() {
        Table inputTable = getAndSetInputTable();
        Table apsTable = getAndSetApsTable();
        Table accountTable = getAndSetAccountTable();
        CreateCdlEventTableParameters parameters = new CreateCdlEventTableParameters(inputTable.getName(),
                apsTable.getName(), accountTable.getName());
        return parameters;
    }

    private Table getAndSetAccountTable() {
        Table accountTable = dataCollectionProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                TableRoleInCollection.ConsolidatedAccount);
        if (accountTable == null) {
            throw new RuntimeException("There's no Account table!");
        }
        int changedCount = 0;
        List<Attribute> attributes = accountTable.getAttributes();
        List<String> internal = Arrays.asList(ModelingMetadata.INTERNAL_TAG);
        for (Attribute attribute : attributes) {
            if (CollectionUtils.isEmpty(attribute.getTags()) || attribute.getTags().get(0).equals("")) {
                attribute.setTags(internal);
                changedCount++;
            }
        }
        if (changedCount > 0) {
            metadataProxy.updateTable(configuration.getCustomerSpace().toString(), accountTable.getName(),
                    accountTable);
            DataCollection.Version version = dataCollectionProxy
                    .getActiveVersion(configuration.getCustomerSpace().toString());
            dataCollectionProxy.upsertTable(configuration.getCustomerSpace().toString(), accountTable.getName(), //
                    TableRoleInCollection.ConsolidatedAccount, version);
        }
        log.info("The number of attributes having no Tags is=" + changedCount);
        return accountTable;
    }

    private Table getAndSetApsTable() {
        String customerSpace = configuration.getCustomerSpace().toString();
        Table apsTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.AnalyticPurchaseState);
        if (apsTable == null) {
            throw new RuntimeException("There's no AnalyticPurchaseState table!");
        }
        return apsTable;
    }

    private Table getAndSetInputTable() {
        Table inputTable = getObjectFromContext(FILTER_EVENT_TABLE, Table.class);
        if (inputTable == null) {
            throw new RuntimeException("There's no input table found!");
        }
        List<Attribute> attributes = inputTable.getAttributes();
        for (Attribute attribute : attributes) {
            attribute.setApprovedUsage(ApprovedUsage.NONE);
            attribute.setTags(ModelingMetadata.EXTERNAL_TAG);
            String name = attribute.getName();
            if (InterfaceName.Target.name().equalsIgnoreCase(name)
                    || InterfaceName.Event.name().equalsIgnoreCase(name)) {
                attribute.setLogicalDataType(LogicalDataType.Event);
            }
        }
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), inputTable.getName(), inputTable);
        return inputTable;
    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, eventTable);
        putStringValueInContext(MATCH_FETCH_ONLY, "true");
    }

}
