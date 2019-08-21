package com.latticeengines.cdl.workflow.steps;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
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
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;

@Component("createCdlEventTableStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CreateCdlEventTableStep extends RunDataFlow<CreateCdlEventTableConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CreateCdlEventTableStep.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MatchDataCloudWorkflow matchDataCloud;

    private DataCollection.Version version;
    private String accountFeatureTable;

    @Override
    public void onConfigurationInitialized() {
        CreateCdlEventTableConfiguration configuration = getConfiguration();
        if (StringUtils.isBlank(configuration.getTargetTableName())) {
            String targetTableName = NamingUtils.timestampWithRandom("CdlEventTable");
            configuration.setTargetTableName(targetTableName);
            log.info("Generated a new target table name: " + targetTableName);
        }
        version = configuration.getDataCollectionVersion();
        if (version == null) {
            version = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
            log.info("Read inactive version from workflow context: " + version);
        } else {
            log.info("Use the version specified in configuration: " + version);
        }
        configuration.setApplyTableProperties(true);
        configuration.setDataFlowParams(createDataFlowParameters());
    }

    private DataFlowParameters createDataFlowParameters() {
        Table inputTable = getAndSetInputTable();
        if (inputTable == null) {
            throw new IllegalArgumentException("No input table.");
        }
        Table apsTable = getAndSetApsTable();
        Table accountTable = getAndSetAccountTable();

        CreateCdlEventTableParameters parameters = new CreateCdlEventTableParameters(inputTable.getName(),
                apsTable != null ? apsTable.getName() : null, accountTable.getName());
        parameters.setEventColumn(configuration.getEventColumn());
        return parameters;
    }

    private Table getAndSetAccountTable() {
        TableRoleInCollection roleInCollection = TableRoleInCollection.ConsolidatedAccount;
        if (configuration.isUseAccountFeature()) {
            accountFeatureTable = getAccountFeatureTable();
            if (StringUtils.isNotEmpty(accountFeatureTable)) {
                roleInCollection = TableRoleInCollection.AccountFeatures;
                log.info("Use account feature table instead.");
            }
        }
        Table accountTable = dataCollectionProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                roleInCollection, version);
        if (accountTable == null) {
            accountTable = dataCollectionProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                    roleInCollection, version.complement());
            if (accountTable != null) {
                log.info("Found Account table in version " + version.complement());
            }
        } else {
            log.info("Found Account table in version " + version);
        }
        if (accountTable == null) {
            throw new RuntimeException("There's no Account table!");
        }
        int changedCount = 0;
        List<Attribute> attributes = accountTable.getAttributes();
        List<String> internal = Collections.singletonList(ModelingMetadata.INTERNAL_TAG);
        for (Attribute attribute : attributes) {
            if (CollectionUtils.isEmpty(attribute.getTags()) || attribute.getTags().get(0).equals("")) {
                attribute.setTags(internal);
                changedCount++;
            }
        }
        if (changedCount > 0) {
            String customerSpace = configuration.getCustomerSpace().toString();
            boolean updateVersion = false;
            String tableName = dataCollectionProxy.getTableName(customerSpace, roleInCollection, version);
            if (accountTable.getName().equals(tableName)) {
                updateVersion = true;
            }
            boolean updateComplementVersion = false;
            tableName = dataCollectionProxy.getTableName(customerSpace, roleInCollection, version.complement());
            if (accountTable.getName().equals(tableName)) {
                updateComplementVersion = true;
            }

            metadataProxy.updateTable(configuration.getCustomerSpace().toString(), accountTable.getName(),
                    accountTable);
            if (updateVersion) {
                dataCollectionProxy.upsertTable(configuration.getCustomerSpace().toString(), accountTable.getName(), //
                        roleInCollection, version);
            }
            if (updateComplementVersion) {
                dataCollectionProxy.upsertTable(configuration.getCustomerSpace().toString(), accountTable.getName(), //
                        roleInCollection, version.complement());
            }

        }
        log.info("The number of attributes having no Tags is=" + changedCount);
        return accountTable;
    }

    private String getAccountFeatureTable() {
        String customerSpace = getConfiguration().getCustomerSpace().toString();
        Table featureTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.AccountFeatures,
                version);
        if (featureTable == null) {
            featureTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.AccountFeatures,
                    version.complement());
        }
        return featureTable != null ? featureTable.getName() : null;
    }

    private Table getAndSetApsTable() {
        Table apsTable = getApsTable();
        boolean hasCrossSell = configuration.isCrossSell()
                || "true".equalsIgnoreCase(getStringValueFromContext(HAS_CROSS_SELL_MODEL));
        if (hasCrossSell) {
            if (apsTable == null) {
                throw new RuntimeException("There's no AnalyticPurchaseState table!");
            }
        } else {
            apsTable = null;
        }
        return apsTable;
    }

    private Table getApsTable() {
        String customerSpace = configuration.getCustomerSpace().toString();
        Table apsTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.AnalyticPurchaseState,
                version);
        if (apsTable == null) {
            apsTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.AnalyticPurchaseState,
                    version.complement());
            if (apsTable != null) {
                log.info("Found AnalyticPurchaseState table in version " + version.complement());
            }
        } else {
            log.info("Found AnalyticPurchaseState table in version " + version);
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
            log.warn("There's no cross sell input table found!");
        } else {
            String path = inputTable.getExtracts().get(0).getPath();
            if (!path.endsWith(".avro")) {
                path = path + "/" + "*.avro";
            }
            long count = AvroUtils.count(yarnConfiguration, path);
            log.info(count + " records in cross sell input table " + inputTable.getName() + ":" + path);
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
        }
        return inputTable;
    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        if (StringUtils.isNotEmpty(accountFeatureTable)) {
            putObjectInContext(EVENT_TABLE, eventTable);
            putObjectInContext(MATCH_RESULT_TABLE, eventTable);
            skipEmbeddedWorkflow(getParentNamespace(), matchDataCloud.name(),
                    MatchDataCloudWorkflowConfiguration.class);
        } else {
            putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, eventTable);

        }
        if (!getConfiguration().isExportKeyColumnsOnly()) {
            putStringValueInContext(FILTER_EVENT_TARGET_TABLE_NAME, eventTable.getName());
        }
    }

}
