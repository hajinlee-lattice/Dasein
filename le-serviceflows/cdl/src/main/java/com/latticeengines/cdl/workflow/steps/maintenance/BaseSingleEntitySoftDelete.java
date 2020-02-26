package com.latticeengines.cdl.workflow.steps.maintenance;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
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
                dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), batchStore, inactive);
            }
        }
        if (processSystemBatchStore()) {
            table = metadataProxy.getTable(customerSpace.toString(), getSystemBatchStoreName());
            if (table == null) {
                log.warn("Did not generate new table for " + systemBatchStore);
                dataCollectionProxy.unlinkTables(customerSpace.toString(), systemBatchStore, inactive);
            } else {
                dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), systemBatchStore, inactive);
            }
        }
    }

    protected String getBatchStoreName() {
        return TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion);
    }

    private String getSystemBatchStoreName() {
        return TableUtils.getFullTableName(systemBatchStoreTablePrefix, pipelineVersion);
    }

    TransformationStepConfig mergeSoftDelete(List<Action> softDeleteActions) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        softDeleteActions.forEach(action -> {
            DeleteActionConfiguration configuration = (DeleteActionConfiguration) action.getActionConfiguration();
            addBaseTables(step, configuration.getDeleteDataTable());
        });
        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(true);
        config.setJoinKey(InterfaceName.AccountId.name());
        config.setAddTimestamps(false);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    TransformationStepConfig softDelete(int mergeSoftDeleteStep) {
        return softDelete(mergeSoftDeleteStep, masterTable, null, batchStoreTablePrefix);
    }

    TransformationStepConfig softDeleteSystemBatchStore(int mergeSoftDeleteStep) {
        return softDelete(mergeSoftDeleteStep, systemMasterTable, InterfaceName.EntityId.name(),
                systemBatchStoreTablePrefix);
    }

    TransformationStepConfig softDeleteSystemBatchStoreByContact(int mergeSoftDeleteStep) {
        return softDelete(mergeSoftDeleteStep, systemMasterTable, InterfaceName.ContactId.name(),
                InterfaceName.EntityId.name(), systemBatchStoreTablePrefix);
    }

    private TransformationStepConfig softDelete(int mergeSoftDeleteStep, Table masterTable, String sourceIdColumn,
            String tgtTablePrefix) {
        return softDelete(mergeSoftDeleteStep, masterTable, InterfaceName.AccountId.name(), sourceIdColumn,
                tgtTablePrefix);
    }

    private TransformationStepConfig softDelete(int mergeSoftDeleteStep, Table masterTable, String joinColumn,
                                                String sourceIdColumn, String tgtTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_SOFT_DELETE_TXFMR);
        step.setInputSteps(Collections.singletonList(mergeSoftDeleteStep));
        setTargetTable(step, tgtTablePrefix);
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
