package com.latticeengines.cdl.workflow.steps.maintenance;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;

public abstract class BaseSingleEntitySoftDelete<T extends BaseProcessEntityStepConfiguration>
        extends BaseTransformWrapperStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseSingleEntitySoftDelete.class);

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private LivySessionService sessionService;

    @Inject
    private SparkJobService sparkJobService;

    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    protected Table masterTable;
    private Table systemMasterTable;
    protected BusinessEntity entity;
    protected TableRoleInCollection batchStore;
    private TableRoleInCollection systemBatchStore;
    private String batchStoreTablePrefix;
    private String systemBatchStoreTablePrefix;
    List<Action> softDeleteActions;

    protected abstract PipelineTransformationRequest getConsolidateRequest();
    protected abstract boolean skipRegisterBatchStore();

    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        softDeleteActions = getListObjectFromContext(SOFT_DEELETE_ACTIONS, Action.class);
        entity = configuration.getMainEntity();
        batchStore = BusinessEntity.Transaction.equals(entity) ? TableRoleInCollection.ConsolidatedRawTransaction :
                entity.getBatchStore();
        batchStoreTablePrefix = entity.name();
        masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore, active);
        if (processSystemBatchStore()) {
            switch (entity) {
                case Account:
                    systemBatchStore = TableRoleInCollection.SystemAccount;
                    break;
                case Contact:
                    systemBatchStore = TableRoleInCollection.SystemContact;
                    break;
                default:
                    throw new UnsupportedOperationException("No system batch store for " + entity.name());
            }
            systemMasterTable = dataCollectionProxy.getTable(customerSpace.toString(), systemBatchStore, active);
            systemBatchStoreTablePrefix = systemBatchStore.name();
        }
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getConsolidateRequest();
        if (request == null) {
            return null;
        } else {
            return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
        }
    }

    @Override
    protected void onPostTransformationCompleted() {
        if (!skipRegisterBatchStore()) {
            registerBatchStore();
        }
        updateEntityValueMapInContext(PERFORM_SOFT_DELETE, Boolean.TRUE, Boolean.class);
        long recordsBeforeSoftDelete = countRawEntitiesInHdfs(batchStore, active);
        long recordsAfterSoftDelete = countRawEntitiesInHdfs(batchStore, inactive);
        updateEntityValueMapInContext(SOFT_DELETE_RECORD_COUNT, recordsBeforeSoftDelete - recordsAfterSoftDelete,
                Long.class);
    }

    private void enrichTableSchema(Table table, Table inheritTable) {
        Map<String, Attribute> attrsToInherit = new HashMap<>();
        if (inheritTable != null) {
            inheritTable.getAttributes().forEach(attr -> attrsToInherit.putIfAbsent(attr.getName(), attr));
        }
        List<Attribute> newAttrs = table.getAttributes()
                .stream()
                .map(attr -> attrsToInherit.getOrDefault(attr.getName(), attr))
                .collect(Collectors.toList());
        table.setAttributes(newAttrs);
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
    }

    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(entity, key, value, clz);
    }

    protected <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }

    private void registerBatchStore() {
        Table table = metadataProxy.getTable(customerSpace.toString(), getBatchStoreName());
        if (entity.getBatchStore() != null) {
            if (table == null) {
                log.warn("Did not generate new table for " + batchStore);
                dataCollectionProxy.unlinkTables(customerSpace.toString(), batchStore, inactive);
            } else {
                enrichTableSchema(table, masterTable);
                dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), batchStore, inactive);
            }
        }
        if (processSystemBatchStore()) {
            table = metadataProxy.getTable(customerSpace.toString(), getSystemBatchStoreName());
            if (table == null) {
                log.warn("Did not generate new table for " + systemBatchStore);
                dataCollectionProxy.unlinkTables(customerSpace.toString(), systemBatchStore, inactive);
            } else {
                enrichTableSchema(table, systemMasterTable);
                dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), systemBatchStore, inactive);
            }
        }
    }

    void setOutputTablePrefix(List<TransformationStepConfig> steps) {
        TransformationStepConfig lastStep = steps.get(steps.size() - 1);
        setTargetTable(lastStep, batchStoreTablePrefix);
        if (hasSystemStore()) {
            TransformationStepConfig secondLastStep = steps.get(steps.size() - 2);
            setTargetTable(secondLastStep, systemBatchStoreTablePrefix);
        }
    }

    protected String getBatchStoreName() {
        return TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion);
    }

    private String getSystemBatchStoreName() {
        return TableUtils.getFullTableName(systemBatchStoreTablePrefix, pipelineVersion);
    }

    List<Action> filterSoftDeleteActions(List<Action> softDeleteActions, BusinessEntity idEntity) {
        return softDeleteActions.stream().filter(action -> {
            DeleteActionConfiguration configuration = (DeleteActionConfiguration) action.getActionConfiguration();
            return configuration.hasEntity(entity) && idEntity.equals(configuration.getIdEntity());
        }).collect(Collectors.toList());
    }

    TransformationStepConfig mergeSoftDelete(List<Action> softDeleteActions, String idColumn) {
        return mergeSoftDelete(softDeleteActions, idColumn, -1);
    }

    TransformationStepConfig mergeSoftDelete(List<Action> softDeleteActions, String idColumn, int extraIdStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        softDeleteActions.forEach(action -> {
            DeleteActionConfiguration configuration = (DeleteActionConfiguration) action.getActionConfiguration();
            addBaseTables(step, configuration.getDeleteDataTable());
        });
        if (extraIdStep >= 0) {
            step.setInputSteps(Collections.singletonList(extraIdStep));
        }
        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(true);
        config.setJoinKey(idColumn);
        config.setAddTimestamps(false);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    TransformationStepConfig softDelete(int mergeSoftDeleteStep) {
        return softDelete(mergeSoftDeleteStep, masterTable, null);
    }

    TransformationStepConfig softDeleteSystemBatchStore(int mergeSoftDeleteStep) {
        return softDelete(mergeSoftDeleteStep, systemMasterTable, InterfaceName.EntityId.name());
    }

    TransformationStepConfig softDeleteSystemBatchStoreByContact(int mergeSoftDeleteStep) {
        return softDelete(mergeSoftDeleteStep, systemMasterTable, InterfaceName.ContactId.name(),
                InterfaceName.EntityId.name());
    }

    boolean hasSystemStore() {
        return systemMasterTable != null;
    }

    String getJoinColumn() {
        return BusinessEntity.Contact.equals(entity) ? InterfaceName.ContactId.name() : InterfaceName.AccountId.name();
    }

    private TransformationStepConfig softDelete(int mergeSoftDeleteStep, Table masterTable, String sourceIdColumn) {
        return softDelete(mergeSoftDeleteStep, masterTable, getJoinColumn(), sourceIdColumn);
    }

    private TransformationStepConfig softDelete(int mergeSoftDeleteStep, Table masterTable, String joinColumn,
                                                String sourceIdColumn) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_SOFT_DELETE_TXFMR);
        step.setInputSteps(Collections.singletonList(mergeSoftDeleteStep));
        if (masterTable != null) {
            log.info("Add masterTable=" + masterTable.getName());
            addBaseTables(step, masterTable.getName());
        } else {
            throw new IllegalArgumentException("The master table is empty for soft delete!");
        }
        SoftDeleteConfig softDeleteConfig = new SoftDeleteConfig();
        softDeleteConfig.setDeleteSourceIdx(0);
        softDeleteConfig.setIdColumn(joinColumn);
        if (StringUtils.isNotEmpty(sourceIdColumn)) {
            softDeleteConfig.setSourceIdColumn(sourceIdColumn);
        }
        step.setConfiguration(appendEngineConf(softDeleteConfig, lightEngineConfig()));
        return step;
    }

    private long countRawEntitiesInHdfs(TableRoleInCollection tableRole, DataCollection.Version version) {
        String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, version);
        if (StringUtils.isBlank(tableName)) {
            return 0L;
        }
        Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
        if (table == null) {
            log.error("Cannot find table " + tableName);
            return 0L;
        }
        String hdfsPath = table.getExtracts().get(0).getPath();
        hdfsPath = PathUtils.toAvroGlob(hdfsPath);
        log.info("Count records in HDFS " + hdfsPath);
        Long result = SparkUtils.countRecordsInGlobs(sessionService, sparkJobService, yarnConfiguration, hdfsPath);
        log.info(String.format("Table role %s version %s has %d entities.", tableRole.name(), version.name(), result));
        return result;
    }

    protected boolean processSystemBatchStore() {
        return false;
    }

}
