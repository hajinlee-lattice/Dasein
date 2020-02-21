package com.latticeengines.cdl.workflow.steps.legacydelete;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedRawTransaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodDataCleanerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodDateConvertorConfig;
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
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.LegacyDeleteJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component(LegacyDeleteByUploadStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LegacyDeleteByUploadStep extends BaseTransformWrapperStep<LegacyDeleteByUploadStepConfiguration> {

    static final String BEAN_NAME = "legacyDeleteByUploadStep";

    private static Logger log = LoggerFactory.getLogger(LegacyDeleteByUploadStep.class);

    private static int prepareStep, cleanupStep, collectMasterStep, cleanupMasterStep, collectStep, mergeStep,
            partitionStep;

    private static final String CLEANUP_TABLE_PREFIX = "DeleteByFile";

    private static final String TRANSFORMER = "LegacyDeleteTxfmr";

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    private Table masterTable;

    private Table cleanupTable;

    private String cleanupTableName;

    private TableRoleInCollection batchStore;

    private CustomerSpace customerSpace;

    private Set<Action> canMergeActions;//all Account/Contact, type BYUPLOAD_MINDATE transaction legacyDeleteAction

    private List<Action> otherActions;//except type=BYUPLOAD_MINDATE transaction actions

    private void intializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        batchStore = configuration.getEntity().equals(BusinessEntity.Transaction)
                ? ConsolidatedRawTransaction : configuration.getEntity().getBatchStore();
        masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore);
        initialData();
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        intializeConfiguration();
        PipelineTransformationRequest request = generateRequest();
        return transformationProxy.getWorkflowConf(configuration.getCustomerSpace().toString(), request,
                configuration.getPodId());
    }

    private PipelineTransformationRequest generateRequest() {
        if (masterTable == null || (CollectionUtils.isEmpty(canMergeActions) && CollectionUtils.isEmpty(otherActions))) {
            return null;
        }
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("LegacyDeleteByUploadStep");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        List<TransformationStepConfig> steps = new ArrayList<>();
        generateSteps(steps);
        request.setSteps(steps);
        return request;
    }

    private void generateSteps(List<TransformationStepConfig> steps) {
        if (CollectionUtils.isEmpty(canMergeActions)) {
            return;
        }
        try {
            /*
             * type=BYUPLOAD_MINDATE transaction legacyDeleteAction
             * using this logic to create delete steps
             */
            if (CollectionUtils.isNotEmpty(canMergeActions)) {
                boolean cleanupTrx = configuration.getEntity().equals(BusinessEntity.Transaction);
                log.info(String.format("Cleanup Business Entity is Transaction: %b", cleanupTrx));
                partitionStep = -1;
                mergeStep = 0;
                prepareStep = 1;
                cleanupStep = 2;
                collectMasterStep = 3;
                cleanupMasterStep = 4;
                collectStep = 5;

                TransformationStepConfig merge = mergeDelete(canMergeActions, getJoinKey(configuration.getEntity(),
                        CleanupOperationType.BYUPLOAD_MINDATE));
                TransformationStepConfig prepare = addTrxDate(null);
                TransformationStepConfig cleanup = cleanup(null, CleanupOperationType.BYUPLOAD_MINDATE);
                TransformationStepConfig collectMaster = collectMaster();
                TransformationStepConfig cleanupMaster = cleanupMaster();
                TransformationStepConfig dayPeriods = collectDays();
                TransformationStepConfig dailyPartition = partitionDaily();

                steps.add(merge);
                steps.add(prepare);
                steps.add(cleanup);
                steps.add(collectMaster);
                steps.add(cleanupMaster);
                steps.add(dayPeriods);
                steps.add(dailyPartition);
            }
            /*
             * except type = BYUPLOAD_MINDATE transaction legacyDeleteActions
             * those actions can not merge to delete.
             * using this logic to delete one by one
             */
            if (CollectionUtils.isNotEmpty(otherActions)) {
                mergeStep = -1;
                partitionStep = steps.size() - 1;
                for (Action action : otherActions) {
                    LegacyDeleteByUploadActionConfiguration legacyDeleteByUploadActionConfiguration =
                            (LegacyDeleteByUploadActionConfiguration) action.getActionConfiguration();
                    steps.add(addTrxDate(legacyDeleteByUploadActionConfiguration));
                    prepareStep = steps.size() - 1;
                    steps.add(cleanup(legacyDeleteByUploadActionConfiguration,
                            legacyDeleteByUploadActionConfiguration.getCleanupOperationType()));
                    cleanupStep = steps.size() - 1;
                    steps.add(collectMaster());
                    cleanupStep = steps.size() - 1;
                    steps.add(cleanupMaster());
                    cleanupMasterStep = steps.size() - 1;
                    steps.add(collectDays());
                    collectStep = steps.size() - 1;
                    steps.add(partitionDaily());
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig addTrxDate(LegacyDeleteByUploadActionConfiguration legacyDeleteByUploadActionConfiguration) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATE_CONVERTOR);

        if (legacyDeleteByUploadActionConfiguration != null) {//used cannot merge Action, input was tableName
            String deleteName = legacyDeleteByUploadActionConfiguration.getTableName();
            List<String> sourceNames = new ArrayList<>();
            Map<String, SourceTable> baseTables = new HashMap<>();
            SourceTable delete = new SourceTable(deleteName, customerSpace);
            sourceNames.add(deleteName);
            baseTables.put(deleteName, delete);
            step.setBaseSources(sourceNames);
            step.setBaseTables(baseTables);
        }

        if (mergeStep != -1) {//used canMergeActions, input was merge step result
            step.setInputSteps(Collections.singletonList(mergeStep));
        }

        PeriodDateConvertorConfig config = new PeriodDateConvertorConfig();
        config.setTrxTimeField(InterfaceName.TransactionTime.name());
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setTrxDayPeriodField(InterfaceName.TransactionDayPeriod.name());

        step.setConfiguration(JsonUtils.serialize(config));
        step.setTargetTable(getTargetTable());
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
        if (partitionStep == -1) {
            sourceNames.add(masterTable.getName());
            SourceTable sourceTable = new SourceTable(masterTable.getName(), customerSpace);
            sourceTables.put(masterTable.getName(), sourceTable);
            step.setBaseSources(sourceNames);
            step.setBaseTables(sourceTables);
        } else {
            step.setInputSteps(Collections.singletonList(partitionStep));
        }

        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());

        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig cleanupMaster() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_CLEANER);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(collectMasterStep);

        if (partitionStep == -1) {
            String tableSourceName = "MasterTable";
            String sourceTableName = masterTable.getName();
            SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
            List<String> baseSources = Collections.singletonList(tableSourceName);
            step.setBaseSources(baseSources);
            Map<String, SourceTable> baseTables = new HashMap<>();
            baseTables.put(tableSourceName, sourceTable);
            step.setBaseTables(baseTables);
        } else {
            inputSteps.add(partitionStep);
        }
        step.setInputSteps(inputSteps);
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

        if (partitionStep == -1) {
            String tableSourceName = "RawTransaction";
            String sourceTableName = masterTable.getName();
            SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
            List<String> baseSources = Collections.singletonList(tableSourceName);
            step.setBaseSources(baseSources);
            Map<String, SourceTable> baseTables = new HashMap<>();
            baseTables.put(tableSourceName, sourceTable);
            step.setBaseTables(baseTables);
        } else {
           inputSteps.add(partitionStep);
        }
        step.setInputSteps(inputSteps);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig cleanup(LegacyDeleteByUploadActionConfiguration legacyDeleteByUploadActionConfiguration, CleanupOperationType type) {
        TransformationStepConfig step = new TransformationStepConfig();
        BusinessEntity entity = configuration.getEntity();
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(prepareStep);

        if (partitionStep == -1) {
            List<String> sourceNames = new ArrayList<>();
            Map<String, SourceTable> baseTables = new HashMap<>();
            String masterName = masterTable.getName();
            SourceTable source = new SourceTable(masterName, customerSpace);

            sourceNames.add(masterName);
            baseTables.put(masterName, source);
            step.setBaseSources(sourceNames);
            step.setBaseTables(baseTables);
        } else {
            inputSteps.add(partitionStep);
        }
        step.setInputSteps(inputSteps);

        LegacyDeleteJobConfig legacyDeleteJobConfig = new LegacyDeleteJobConfig();
        legacyDeleteJobConfig.setBusinessEntity(entity);
        legacyDeleteJobConfig.setOperationType(type);
        legacyDeleteJobConfig.setJoinedColumns(getJoinedColumns(entity, type));
        legacyDeleteJobConfig.setDeleteSourceIdx(0);

        String configStr = appendEngineConf(legacyDeleteJobConfig, lightEngineConfig());

        step.setTransformer(TRANSFORMER);
        step.setConfiguration(configStr);
        step.setTargetTable(getTargetTable());

        return step;
    }

    private TransformationStepConfig mergeDelete(Set<Action> actionSet, String joinKey) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        actionSet.forEach(action -> {
            LegacyDeleteByUploadActionConfiguration configuration = (LegacyDeleteByUploadActionConfiguration) action.getActionConfiguration();
            addBaseTables(step, configuration.getTableName());
        });
        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(true);
        config.setJoinKey(joinKey);
        config.setAddTimestamps(false);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    private LegacyDeleteJobConfig.JoinedColumns getJoinedColumns(BusinessEntity entity, CleanupOperationType type) {
        LegacyDeleteJobConfig.JoinedColumns joinedColumns = new LegacyDeleteJobConfig.JoinedColumns();
        switch (entity) {
            case Account:
                joinedColumns.setAccountId(InterfaceName.AccountId.name());
                break;
            case Contact:
                joinedColumns.setContactId(InterfaceName.ContactId.name());
                break;
            case Transaction:
                switch (type) {
                    case BYUPLOAD_MINDATE:
                        joinedColumns.setTransactionTime(InterfaceName.TransactionDayPeriod.name());
                        break;
                    case BYUPLOAD_MINDATEANDACCOUNT:
                        joinedColumns.setAccountId(InterfaceName.AccountId.name());
                        joinedColumns.setTransactionTime(InterfaceName.TransactionDayPeriod.name());
                        break;
                    case BYUPLOAD_ACPD:
                        joinedColumns.setAccountId(InterfaceName.AccountId.name());
                        joinedColumns.setContactId(InterfaceName.ContactId.name());
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

    private String getJoinKey(BusinessEntity entity, CleanupOperationType type) {
        switch (entity) {
            case Account:
                return InterfaceName.AccountId.name();
            case Contact:
                return InterfaceName.ContactId.name();
            case Transaction:
                if (type.equals(CleanupOperationType.BYUPLOAD_MINDATE)) {
                    return InterfaceName.TransactionDayPeriod.name();
                }
            default:
                return null;
        }
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
        if (getTableDataLines(cleanupTable) <= 0) {
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

    private TargetTable getTargetTable() {
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(CLEANUP_TABLE_PREFIX);
        return targetTable;
    }

    private void initialData() {
        switch (configuration.getEntity()) {
            case Account:
                canMergeActions = getSetObjectFromContext(ACCOUNT_LEGACY_DELTE_BYUOLOAD_ACTIONS, Action.class);
                break;
            case Contact:
                canMergeActions = getSetObjectFromContext(CONTACT_LEGACY_DELTE_BYUOLOAD_ACTIONS, Action.class);
                break;
            case Transaction:
                Map<CleanupOperationType, Set> actionMap =
                        new HashMap<>(getMapObjectFromContext(TRANSACTION_LEGACY_DELTE_BYUOLOAD_ACTIONS,
                                CleanupOperationType.class, Set.class));
                log.info("actionMap is : {}", JsonUtils.serialize(actionMap));
                otherActions = new ArrayList<>();
                if (actionMap.containsKey(CleanupOperationType.BYUPLOAD_MINDATE)) {
                    canMergeActions = JsonUtils.convertSet(actionMap.get(CleanupOperationType.BYUPLOAD_MINDATE),
                            Action.class);
                    actionMap.remove(CleanupOperationType.BYUPLOAD_MINDATE);
                }
                for (Set actionSet : actionMap.values()) {
                    otherActions.addAll(JsonUtils.convertSet(actionSet, Action.class));
                }
                break;
            default:
                break;
        }
    }

    protected void addBaseTables(TransformationStepConfig step, String... sourceTableNames) {
        if (customerSpace == null) {
            throw new IllegalArgumentException("Have not set customerSpace.");
        }
        List<String> baseSources = step.getBaseSources();
        if (CollectionUtils.isEmpty(baseSources)) {
            baseSources = new ArrayList<>();
        }
        Map<String, SourceTable> baseTables = step.getBaseTables();
        if (MapUtils.isEmpty(baseTables)) {
            baseTables = new HashMap<>();
        }
        for (String sourceTableName : sourceTableNames) {
            SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
            baseSources.add(sourceTableName);
            baseTables.put(sourceTableName, sourceTable);
        }
        step.setBaseSources(baseSources);
        step.setBaseTables(baseTables);
    }

    protected Long getTableDataLines(Table table) {
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
}
