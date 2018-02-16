package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import com.latticeengines.common.exposed.util.NamingUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
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

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Value("${dataplatform.queue.scheme}")
    private String queueScheme;

    @Override
    public void onConfigurationInitialized() {
        CreateCdlEventTableConfiguration configuration = getConfiguration();
        if (StringUtils.isBlank(configuration.getTargetTableName())) {
            String targetTableName = NamingUtils.timestamp("CdlEventTable");
            configuration.setTargetTableName(targetTableName);
            log.info("Read target table name from context: " + targetTableName);
        }
        configuration.setApplyTableProperties(true);
        configuration.setDataFlowParams(createDataFlowParameters());
    }

    private DataFlowParameters createDataFlowParameters() {
        Table inputTable = getAndSetInputTable();
        Table apsTable = getAndSetApsTable();
        Table accountTable = getAndSetAccountTable();
        CreateCdlEventTableParameters parameters = new CreateCdlEventTableParameters(inputTable.getName(),
                apsTable.getName(), accountTable.getName());
        parameters.setEventTable(configuration.getEventColumn());
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
            String inputTableName = getStringValueFromContext(FILTER_EVENT_TARGET_TABLE_NAME);
            if (StringUtils.isNotBlank(inputTableName)) {
                inputTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), inputTableName);
            }
        }
        if (inputTable == null) {
            throw new RuntimeException("There's no input table found!");
        }
        long count = AvroUtils.count(yarnConfiguration, inputTable.getExtracts().get(0).getPath());
        log.info(count + " records in input table " + inputTable.getName() + ":" + inputTable.getExtracts().get(0).getPath());
        List<Attribute> attributes = inputTable.getAttributes();
        for (Attribute attribute : attributes) {
            attribute.setApprovedUsage(ApprovedUsage.NONE);
            attribute.setTags(ModelingMetadata.EXTERNAL_TAG);
            String name = attribute.getName();
            if (getConfiguration().getEventColumn().equalsIgnoreCase(name)) {
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
