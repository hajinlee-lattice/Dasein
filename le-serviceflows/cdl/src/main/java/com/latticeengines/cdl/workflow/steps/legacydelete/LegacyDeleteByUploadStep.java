package com.latticeengines.cdl.workflow.steps.legacydelete;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedRawTransaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.CleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataCleanerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDateConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.LegacyDeleteByUploadActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteByUploadStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseMultiTransformationStep;

@Component(LegacyDeleteByUploadStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LegacyDeleteByUploadStep extends BaseMultiTransformationStep<LegacyDeleteByUploadStepConfiguration> {

    static final String BEAN_NAME = "legacyDeleteByUploadStep";

    private static Logger log = LoggerFactory.getLogger(LegacyDeleteByUploadStep.class);

    private static int prepareStep, cleanupStep, collectMasterStep, cleanupMasterStep, collectStep;

    private static final String CLEANUP_TABLE_PREFIX = "DeleteByFile";

    private static final String DELETE_TEMPLATE_PREFIX = "DeleteFileTemplate";

    private static final String TRANSFORMER = "CleanupTransformer";

    private static final ObjectMapper OM = new ObjectMapper();

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Value("${pls.cdl.transform.default.cascading.engine}")
    private String defaultEngine;

    @Value("${pls.cdl.transform.cascading.partitions}")
    protected int cascadingPartitions;

    @Value("${pls.cdl.transform.spark.executors}")
    private int sparkExecutors;

    @Value("${pls.cdl.transform.tez.am.mem.gb}")
    private int tezAmMemGb; // requested memory for application master

    @Value("${pls.cdl.transform.tez.task.vcores}")
    private int tezVCores;

    @Value("${pls.cdl.transform.tez.task.mem.gb}")
    private int tezMemGb;

    private Table masterTable;

    private Table cleanupTable;

    private String cleanupTableName;

    private Long tableRows = 0L;

    private TableRoleInCollection batchStore;

    private CustomerSpace customerSpace;

    private int scalingMultiplier = 1;

    private List<Action> actionList = null;

    @Override
    protected void intializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        batchStore = configuration.getEntity().equals(BusinessEntity.Transaction)
                ? ConsolidatedRawTransaction : configuration.getEntity().getBatchStore();
        masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore);
        Map<BusinessEntity, Set> actionMap = getMapObjectFromContext(LEGACY_DELTE_BYUOLOAD_ACTIONS,
                BusinessEntity.class, Set.class);
        log.info("actionMap is : {}", JsonUtils.serialize(actionMap));
        if (actionMap == null || !actionMap.containsKey(configuration.getEntity())) {
            return;
        }
        actionList = new ArrayList<>(JsonUtils.convertSet(actionMap.get(configuration.getEntity()), Action.class));
    }

    @Override
    protected boolean iteratorContinued(TransformationProgress progress, int index) {
        if (progress == null && index < actionList.size() - 1) {
            return true;
        }
        if (index >= actionList.size() - 1) {
            return false;
        }
        cleanupTableName = TableUtils.getFullTableName(CLEANUP_TABLE_PREFIX, progress.getVersion());
        cleanupTable = metadataProxy.getTable(customerSpace.toString(), cleanupTableName);
        if (cleanupTable == null) {
            log.info("cleanupTable is empty.");
            return false;
        }
        log.info("result table Name is " + cleanupTable.getName());
        tableRows = getTableDataLines(cleanupTable);
        log.info("tableRows is: {}.", tableRows);
        if (tableRows <= 0L) {
            return false;
        }
        masterTable = cleanupTable;
        return true;
    }

    @Override
    protected PipelineTransformationRequest generateRequest(TransformationProgress progress, int index) {
        Action action = actionList.get(index);
        if (action == null) {
            return null;
        }
        return generateRequest((LegacyDeleteByUploadActionConfiguration) action.getActionConfiguration());
    }

    private PipelineTransformationRequest generateRequest(LegacyDeleteByUploadActionConfiguration legacyDeleteByUploadActionConfiguration) {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("LegacyDeleteByUploadStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);
            boolean cleanupTrx = configuration.getEntity().equals(BusinessEntity.Transaction);
            log.info(String.format("Cleanup Business Entity is Transaction: %b", cleanupTrx));
            prepareStep = 0;
            cleanupStep = 1;
            collectMasterStep = 2;
            cleanupMasterStep = 3;
            collectStep = 4;

            List<TransformationStepConfig> steps = new ArrayList<>();
            if (cleanupTrx) {
                TransformationStepConfig prepare = addTrxDate(legacyDeleteByUploadActionConfiguration);
                TransformationStepConfig cleanup = cleanup(cleanupTrx, legacyDeleteByUploadActionConfiguration);
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
                TransformationStepConfig cleanup = cleanup(cleanupTrx, legacyDeleteByUploadActionConfiguration);
                steps.add(cleanup);
            }

            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig addTrxDate(LegacyDeleteByUploadActionConfiguration legacyDeleteByUploadActionConfiguration) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATE_CONVERTOR);

        String deleteName = legacyDeleteByUploadActionConfiguration.getTableName();
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

    private TransformationStepConfig cleanup(boolean cleanupTrx, LegacyDeleteByUploadActionConfiguration legacyDeleteByUploadActionConfiguration) {
        TransformationStepConfig step = new TransformationStepConfig();
        BusinessEntity entity = configuration.getEntity();

        if (cleanupTrx) {
            step.setInputSteps(Collections.singletonList(prepareStep));
        }

        List<String> sourceNames = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();
        if (!cleanupTrx) {
            String deleteName = legacyDeleteByUploadActionConfiguration.getTableName();
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
        config.setOperationType(legacyDeleteByUploadActionConfiguration.getCleanupOperationType());
        config.setTransformer(TRANSFORMER);
        config.setBaseJoinedColumns(getJoinedColumns(config.getBusinessEntity(),
                legacyDeleteByUploadActionConfiguration.getCleanupOperationType()));
        config.setDeleteJoinedColumns(getJoinedColumns(config.getBusinessEntity(),
                legacyDeleteByUploadActionConfiguration.getCleanupOperationType()));

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

    private CleanupConfig.JoinedColumns getJoinedColumns(BusinessEntity entity, CleanupOperationType type) {
        CleanupConfig.JoinedColumns joinedColumns = new CleanupConfig.JoinedColumns();
        boolean enableEntityMatch = batonService.isEntityMatchEnabled(customerSpace);
        InterfaceName accountId = enableEntityMatch ? InterfaceName.CustomerAccountId : InterfaceName.AccountId;
        InterfaceName contactId = enableEntityMatch ? InterfaceName.CustomerContactId : InterfaceName.ContactId;
        switch (entity) {
            case Account:
                joinedColumns.setAccountId(accountId.name());
                break;
            case Contact:
                joinedColumns.setContactId(contactId.name());
                break;
            case Transaction:
                switch (type) {
                    case BYUPLOAD_MINDATE:
                        joinedColumns.setTransactionTime(InterfaceName.TransactionDayPeriod.name());
                        break;
                    case BYUPLOAD_MINDATEANDACCOUNT:
                        joinedColumns.setAccountId(accountId.name());
                        joinedColumns.setTransactionTime(InterfaceName.TransactionDayPeriod.name());
                        break;
                    case BYUPLOAD_ACPD:
                        joinedColumns.setAccountId(accountId.name());
                        joinedColumns.setContactId(contactId.name());
                        joinedColumns.setProductId(InterfaceName.ProductId.name());
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
        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            paths.add(PathUtils.toAvroGlob(extract.getPath()));
        }
        for (String path : paths) {
            lines += AvroUtils.count(yarnConfiguration, path);
        }
        return lines;
    }

    protected String appendEngineConf(TransformerConfig conf,
                                      TransformationFlowParameters.EngineConfiguration engineConf) {
        ObjectNode on = OM.valueToTree(conf);
        on.set("EngineConfig", OM.valueToTree(engineConf));
        return JsonUtils.serialize(on);
    }

    protected TransformationFlowParameters.EngineConfiguration lightEngineConfig() {
        if ("FLINK".equalsIgnoreCase(defaultEngine) && scalingMultiplier == 1) {
            TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
            engineConf.setEngine("FLINK");
            engineConf.setPartitions(cascadingPartitions);
            return engineConf;
        } else {
            return heavyEngineConfig();
        }
    }

    private TransformationFlowParameters.EngineConfiguration heavyEngineConfig() {
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setEngine("TEZ");
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put("tez.task.resource.cpu.vcores", String.valueOf(tezVCores));
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(tezMemGb * 1024));
        jobProperties.put("tez.am.resource.memory.mb", String.valueOf(tezAmMemGb * 1024));
        jobProperties.put("tez.grouping.split-count", String.valueOf(2 * cascadingPartitions * scalingMultiplier));
        jobProperties.put("mapreduce.job.reduces", String.valueOf(cascadingPartitions * scalingMultiplier));
        jobProperties.put("spark.dynamicAllocation.maxExecutors", String.valueOf(sparkExecutors * scalingMultiplier));
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions * scalingMultiplier);
        engineConf.setScalingMultiplier(scalingMultiplier);
        return engineConf;
    }

    private boolean noImport() {
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        return MapUtils.isEmpty(entityImportsMap) || !entityImportsMap.containsKey(configuration.getEntity());
    }

    protected void onPostTransformationCompleted() {
        if (cleanupTable == null) {
            return;
        }
        if (batchStore.equals(TableRoleInCollection.ConsolidatedRawTransaction)) {
            return;
        }
        if (tableRows <= 0) {
            if (noImport()) {
                log.error("cannot clean up all batchStore with no import.");
                throw new IllegalStateException("cannot clean up all batchStore with no import, PA failed");
            }
            log.info("Result table is empty, remove " + batchStore.name() + " from data collection!");
            dataCollectionProxy.resetTable(configuration.getCustomerSpace().toString(), batchStore);
            return;
        }
        DataCollection.Version version = getObjectFromContext(CDL_INACTIVE_VERSION,
                DataCollection.Version.class);
        DynamoDataUnit dataUnit = null;
        if (batchStore.equals(BusinessEntity.Account.getBatchStore())) {
            // if replaced account batch store, need to link dynamo table
            String oldBatchStoreName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore,
                    version);
            dataUnit = (DynamoDataUnit) dataUnitProxy.getByNameAndType(customerSpace.toString(), oldBatchStoreName,
                    DataUnit.StorageType.Dynamo);
            if (dataUnit != null) {
                dataUnit.setLinkedTable(StringUtils.isBlank(dataUnit.getLinkedTable()) ? //
                        dataUnit.getName() : dataUnit.getLinkedTable());
                dataUnit.setName(cleanupTableName);
            }
        }
        dataCollectionProxy.upsertTable(customerSpace.toString(), cleanupTableName, batchStore, version);
        if (dataUnit != null) {
            dataUnitProxy.create(customerSpace.toString(), dataUnit);
        }
    }
}
