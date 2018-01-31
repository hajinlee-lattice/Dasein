package com.latticeengines.cdl.workflow.steps.maintenance;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedRawTransaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataCleanerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDateConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.CleanupByUploadWrapperConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component("cleanupByUploadStep")
public class CleanupByUploadStep extends BaseTransformWrapperStep<CleanupByUploadWrapperConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CleanupByUploadStep.class);

    private static int prepareStep, cleanupStep, collectMasterStep, cleanupMasterStep, collectStep;

    private static final String CLEANUP_TABLE_PREFIX = "DeleteByFile";

    private static final String DELETE_TEMPLATE_PREFIX = "DeleteFileTemplate";

    private static final String TRANSFORMER = "CleanupTransformer";

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private CleanupByUploadConfiguration cleanupByUploadConfiguration;

    private Long totalRecords;

    private Table masterTable;

    private TableRoleInCollection batchStore;

    private CustomerSpace customerSpace;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        intializeConfiguration();
        PipelineTransformationRequest request = generateRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String cleanupTableName = TableUtils.getFullTableName(CLEANUP_TABLE_PREFIX, pipelineVersion);
        String customerSpace = configuration.getCustomerSpace().toString();
        Table cleanupTable = metadataProxy.getTable(customerSpace, cleanupTableName);
        if (cleanupTable != null) {
            log.info("result table Name is " + cleanupTable.getName());
            createDeleteReport(getTableDataLines(cleanupTable));
            if (!batchStore.equals(TableRoleInCollection.ConsolidatedRawTransaction)) {
                DataCollection.Version version = dataCollectionProxy.getActiveVersion(customerSpace);
                dataCollectionProxy.upsertTable(configuration.getCustomerSpace().toString(), cleanupTableName,
                        batchStore, version);
            }
        }
    }

    private void createDeleteReport(Long currentRows) {
        ObjectNode json = JsonUtils.createObjectNode();
        json.put(cleanupByUploadConfiguration.getEntity().name() + "_Deleted", totalRecords - currentRows);
        Report report = createReport(json.toString(), ReportPurpose.MAINTENANCE_OPERATION_SUMMARY, UUID.randomUUID().toString());
        registerReport(configuration.getCustomerSpace(), report);
    }

    private void intializeConfiguration() {
        if (configuration.getMaintenanceOperationConfiguration() == null) {
            throw new RuntimeException("Cleanup by upload configuration is NULL!");
        }
        if (configuration.getMaintenanceOperationConfiguration() instanceof CleanupByUploadConfiguration) {
            cleanupByUploadConfiguration = (CleanupByUploadConfiguration) configuration
                    .getMaintenanceOperationConfiguration();
        } else {
            throw new RuntimeException("Cleanup by upload configuration is not expected!");
        }
        customerSpace = configuration.getCustomerSpace();
        batchStore = cleanupByUploadConfiguration.getEntity().equals(BusinessEntity.Transaction) ?
                        ConsolidatedRawTransaction : cleanupByUploadConfiguration.getEntity().getBatchStore();
        if (cleanupByUploadConfiguration.getEntity().equals(BusinessEntity.Transaction)) {
            masterTable = dataCollectionProxy.getTable(customerSpace.toString(),
                    ConsolidatedRawTransaction);
        } else {
            masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore);
        }
        if (masterTable == null) {
            throw new RuntimeException(
                    String.format("master table in collection shouldn't be null when customer space %s, role %s",
                            customerSpace.toString(), batchStore));
        }
        totalRecords = getTableDataLines(masterTable);
    }

    private PipelineTransformationRequest generateRequest() {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("CleanupByUploadStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);
            boolean cleanupTrx = cleanupByUploadConfiguration.getEntity().equals(BusinessEntity.Transaction);
            log.info(String.format("Cleanup Business Entity is Transaction: %b", cleanupTrx));
            prepareStep = 0;
            cleanupStep = 1;
            collectMasterStep = 2;
            cleanupMasterStep = 3;
            collectStep = 4;



            List<TransformationStepConfig> steps = new ArrayList<>();
            if (cleanupTrx) {
                TransformationStepConfig prepare = addTrxDate();
                TransformationStepConfig cleanup = cleanup(cleanupTrx);
                TransformationStepConfig collectMaster = collectMaster();
                TransformationStepConfig cleanupMaster = cleanupMaster();
                TransformationStepConfig dayPeriods = collectDays();
                TransformationStepConfig dailyPartition = partitionDaily();

                steps.add(prepare);
                steps.add(cleanup);
                steps.add(collectMaster);
                steps.add(cleanupMaster);
                steps.add(dayPeriods);
                steps.add(dailyPartition);
            } else {
                TransformationStepConfig cleanup = cleanup(cleanupTrx);
                steps.add(cleanup);
            }

            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig addTrxDate() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATE_CONVERTOR);

        String deleteName = cleanupByUploadConfiguration.getTableName();
        List<String> sourceNames = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();
        SourceTable delete = new SourceTable(deleteName, customerSpace);
        sourceNames.add(deleteName);
        baseTables.put(deleteName, delete);

        PeriodDateConvertorConfig config = new PeriodDateConvertorConfig();
        config.setTrxTimeField(InterfaceName.TransactionTime.name());
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setTrxDayPeriodField(InterfaceName.TransactionDayPeriod.name());

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(DELETE_TEMPLATE_PREFIX);

        step.setBaseSources(sourceNames);
        step.setBaseTables(baseTables);
        step.setConfiguration(JsonUtils.serialize(config));
        step.setTargetTable(targetTable);
        return step;
    }

    private TransformationStepConfig collectDays() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(cleanupStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());

        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig collectMaster() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);

        List<String> sourceNames = new ArrayList<>();
        Map<String, SourceTable> sourceTables = new HashMap<>();
        sourceNames.add(masterTable.getName());
        SourceTable sourceTable = new SourceTable(masterTable.getName(), customerSpace);
        sourceTables.put(masterTable.getName(), sourceTable);

        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());

        step.setBaseSources(sourceNames);
        step.setBaseTables(sourceTables);
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig cleanupMaster() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_CLEANER);
        step.setInputSteps(Collections.singletonList(collectMasterStep));

        String tableSourceName = "MasterTable";
        String sourceTableName = masterTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        PeriodDataCleanerConfig config = new PeriodDataCleanerConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }


    private TransformationStepConfig partitionDaily() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(collectStep);
        inputSteps.add(cleanupStep);
        step.setInputSteps(inputSteps);

        String tableSourceName = "RawTransaction";
        String sourceTableName = masterTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig cleanup(boolean cleanupTrx) {
        TransformationStepConfig step = new TransformationStepConfig();
        BusinessEntity entity = cleanupByUploadConfiguration.getEntity();

        if (cleanupTrx) {
            step.setInputSteps(Collections.singletonList(prepareStep));
        }

        List<String> sourceNames = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();
        if (!cleanupTrx) {
            String deleteName = cleanupByUploadConfiguration.getTableName();
            SourceTable delete = new SourceTable(deleteName, customerSpace);
            sourceNames.add(deleteName);
            baseTables.put(deleteName, delete);
        }
        String masterName = masterTable.getName();
        SourceTable source = new SourceTable(masterName, customerSpace);

        sourceNames.add(masterName);
        baseTables.put(masterName, source);

        CleanupConfig config = new CleanupConfig();
        config.setBusinessEntity(entity);
        config.setOperationType(cleanupByUploadConfiguration.getCleanupOperationType());
        config.setTransformer(TRANSFORMER);
        config.setBaseJoinedColumns(getJoinedColumns(config.getBusinessEntity(),
                cleanupByUploadConfiguration.getCleanupOperationType(), masterTable));
        config.setDeleteJoinedColumns(getJoinedColumns(config.getBusinessEntity(),
                cleanupByUploadConfiguration.getCleanupOperationType(), null));

        String configStr = appendEngineConf(config, lightEngineConfig());
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(CLEANUP_TABLE_PREFIX);

        step.setBaseSources(sourceNames);
        step.setBaseTables(baseTables);
        step.setTransformer(TRANSFORMER);
        step.setConfiguration(configStr);
        step.setTargetTable(targetTable);

        return step;
    }

    private CleanupConfig.JoinedColumns getJoinedColumns(BusinessEntity entity, CleanupOperationType type,
                                                         Table masterTable) {
        CleanupConfig.JoinedColumns joinedColumns = new CleanupConfig.JoinedColumns();
        switch (entity) {
            case Account:
                joinedColumns.setAccountId(
                        masterTable == null ? InterfaceName.AccountId.name() :
                                masterTable.getAttribute(InterfaceName.AccountId).getName());
                break;
            case Contact:
                joinedColumns.setContactId(
                        masterTable == null ? InterfaceName.ContactId.name() :
                                masterTable.getAttribute(InterfaceName.ContactId).getName());
                break;
            case Transaction:
                switch (type) {
                    case BYUPLOAD_MINDATE:
                        joinedColumns.setTransactionTime(InterfaceName.TransactionDayPeriod.name());
                        break;
                    case BYUPLOAD_MINDATEANDACCOUNT:
                        joinedColumns.setAccountId(
                                masterTable == null ? InterfaceName.AccountId.name() :
                                        masterTable.getAttribute(InterfaceName.AccountId).getName());
                        joinedColumns.setTransactionTime(InterfaceName.TransactionDayPeriod.name());
                        break;
                    case BYUPLOAD_ACPD:
                        joinedColumns.setAccountId(
                                masterTable == null ? InterfaceName.AccountId.name() :
                                        masterTable.getAttribute(InterfaceName.AccountId).getName());
                        joinedColumns.setContactId(
                                masterTable == null ? InterfaceName.ContactId.name() :
                                        masterTable.getAttribute(InterfaceName.ContactId).getName());
                        joinedColumns.setProductId(
                                masterTable == null ? InterfaceName.ProductId.name() :
                                        masterTable.getAttribute(InterfaceName.ProductId).getName());
                        joinedColumns.setTransactionTime(InterfaceName.TransactionDayPeriod.name());
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        return joinedColumns;
    }

    private Long getTableDataLines(Table table) {
        if (table == null || table.getExtracts() == null) {
            return 0L;
        }
        Long lines = 0L;
        for (Extract extract : table.getExtracts()) {
            lines += extract.getProcessedRecords();
        }
        return lines;
    }

}
