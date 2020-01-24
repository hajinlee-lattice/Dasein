package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_SYSTEM_BATCH_TXMFR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_UPSERT_TXMFR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.cdl.AttributeLimit;
import com.latticeengines.domain.exposed.cdl.DataLimit;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeSystemBatchConfig;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;
import com.latticeengines.domain.exposed.spark.common.UpsertConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;

public abstract class BaseSingleEntityMergeImports<T extends BaseProcessEntityStepConfiguration>
        extends BaseMergeImports<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseSingleEntityMergeImports.class);

    protected String inputMasterTableName;
    protected String diffTableName;
    protected Table masterTable;
    protected Table systemBatchTable;
    protected String systemBatchTableName;

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    private List<BusinessEntity> businessEntities = Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact,
            BusinessEntity.Product, BusinessEntity.Transaction);

    @Override
    protected void onPostTransformationCompleted() {
        registerSystemBatchStore();
        registerBatchStore();
        generateDiffReport();

        diffTableName = getDiffTableName();
        updateEntityValueMapInContext(ENTITY_DIFF_TABLES, diffTableName, String.class);
        addToListInContext(TEMPORARY_CDL_TABLES, diffTableName, String.class);

        if (hasSchemaChange()) {
            List<BusinessEntity> entityList = getListObjectFromContext(ENTITIES_WITH_SCHEMA_CHANGE,
                    BusinessEntity.class);
            if (entityList == null) {
                entityList = new ArrayList<>();
            }
            entityList.add(entity);
            putObjectInContext(ENTITIES_WITH_SCHEMA_CHANGE, entityList);
        }
    }

    protected void registerSystemBatchStore() {
        if (hasSystemBatch) {
            Table table = metadataProxy.getTable(customerSpace.toString(), getSystemBatchStoreName());
            if (table == null) {
                throw new IllegalStateException("Did not generate new table for " + systemBatchStore);
            }
            enrichSystemBatchTableSchema(table);
            dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), systemBatchStore, inactive);
        }
    }

    protected void registerBatchStore() {
        if (entity.getBatchStore() != null) {
            Table table = metadataProxy.getTable(customerSpace.toString(), getBatchStoreName());
            if (table == null) {
                throw new IllegalStateException("Did not generate new table for " + batchStore);
            }
            isDataQuotaLimit(table);
            enrichTableSchema(table);
            dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), batchStore, inactive);
            if (hasSystemBatch) {
                writeSchema(table);
            }
        }
    }

    protected void checkAttributeLimit(String batchStoreName, boolean entityMatch) {
        Table table = metadataProxy.getTable(customerSpace.toString(), batchStoreName);
        if (businessEntities.contains(configuration.getMainEntity())) {
            AttributeLimit limit = getObjectFromContext(ATTRIBUTE_QUOTA_LIMIT, AttributeLimit.class);
            Integer attrQuota = 0;
            Set<String> names = table.getAttributes().stream().map(Attribute::getName).collect(Collectors.toSet());
            Set<String> internalNames = SchemaRepository.getSystemAttributes(configuration.getMainEntity(), entityMatch)
                    .stream().map(InterfaceName::name).collect(Collectors.toSet());
            log.info(String.format("internal attributes %s.", internalNames));
            Set<String> namesExcludeInternal = names.stream().filter(name -> {
                if (internalNames.contains(name)) {
                    return false;
                }
                if (configuration.isEntityMatchEnabled()) {
                    if (InterfaceName.AccountId.name().equals(name) || InterfaceName.ContactId.name().equals(name)) {
                        return false;
                    }
                    return true;
                }
                return true;
            }).collect(Collectors.toSet());
            AttrConfigRequest configRequest = cdlAttrConfigProxy.getAttrConfigByEntity(customerSpace.toString(),
                    configuration.getMainEntity(), true);
            Set<String> nameExcludeInternalAndInactive = namesExcludeInternal;
            if (CollectionUtils.isNotEmpty(configRequest.getAttrConfigs())) {
                Set<String> inactiveNames = configRequest.getAttrConfigs().stream()
                        .filter(config -> !AttrState.Active
                                .equals(config.getPropertyFinalValue(ColumnMetadataKey.State, AttrState.class)))
                        .map(AttrConfig::getAttrName).collect(Collectors.toSet());
                log.info(String.format("inactive attribute %s.", inactiveNames));
                nameExcludeInternalAndInactive = namesExcludeInternal.stream()
                        .filter(name -> !inactiveNames.contains(name)).collect(Collectors.toSet());
            }
            int attrCount = nameExcludeInternalAndInactive.size();
            log.info(String.format("the size of remaining attributes is %s.", attrCount));
            switch (configuration.getMainEntity()) {
            case Account:
                attrQuota = limit.getAccountAttributeQuotaLimit();
                break;
            case Contact:
                attrQuota = limit.getContactAttributeQuotaLimit();
                break;
            default:
                break;
            }
            if (attrCount > attrQuota) {
                throw new LedpException(LedpCode.LEDP_18226,
                        new String[] { attrQuota.toString(), String.valueOf(configuration.getMainEntity()) });
            }
        }
    }

    private void isDataQuotaLimit(Table table) {
        if (businessEntities.contains(configuration.getMainEntity())) {
            List<Extract> extracts = table.getExtracts();
            if (!CollectionUtils.isEmpty(extracts)) {
                Long dataCount = 0L;
                Long dataQuota = 0L;
                DataLimit dataLimit = getObjectFromContext(DATAQUOTA_LIMIT, DataLimit.class);
                switch (configuration.getMainEntity()) {
                case Account:
                    dataQuota = dataLimit.getAccountDataQuotaLimit();
                    break;
                case Contact:
                    dataQuota = dataLimit.getContactDataQuotaLimit();
                    break;
                case Transaction:
                    dataQuota = dataLimit.getTransactionDataQuotaLimit();
                    break;
                default:
                    break;
                }
                for (Extract extract : extracts) {
                    dataCount = dataCount + extract.getProcessedRecords();
                    log.info("stored " + configuration.getMainEntity() + " data is " + dataCount);
                    if (dataQuota < dataCount)
                        throw new IllegalStateException("the " + configuration.getMainEntity() + " data quota limit is "
                                + dataQuota + ", The data you uploaded has exceeded the limit.");
                }
                log.info("stored data is " + dataCount + ", the " + configuration.getMainEntity() + "data limit is "
                        + dataQuota);
            }
        }
    }

    /**
     * Detect schema changes that requires a rebuild
     */
    private boolean hasSchemaChange() {
        Table oldBatchStore = dataCollectionProxy.getTable(customerSpace.toString(), batchStore, active);
        if (oldBatchStore == null) {
            return true;
        }
        Table newBatchStore = dataCollectionProxy.getTable(customerSpace.toString(), batchStore, inactive);

        // check attributes
        Set<String> oldAttrs = new HashSet<>(Arrays.asList(oldBatchStore.getAttributeNames()));
        Set<String> newAttrs = new HashSet<>(Arrays.asList(newBatchStore.getAttributeNames()));
        // in new but not old
        Set<String> diffAttrs1 = newAttrs.stream().filter(attr -> !oldAttrs.contains(attr)).collect(Collectors.toSet());
        // in old but not new
        Set<String> diffAttrs2 = oldAttrs.stream().filter(attr -> !newAttrs.contains(attr)).collect(Collectors.toSet());
        if (!diffAttrs1.isEmpty() || !diffAttrs2.isEmpty()) {
            log.info("These attributes are in the active batch store but not the inactive one: " + diffAttrs2);
            log.info("These attributes are in the inactive batch store but not the active one: " + diffAttrs1);
            return true;
        }

        return false;
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        boolean isEntityMatchRematch = configuration.isEntityMatchEnabled()
                && Boolean.TRUE.equals(getObjectFromContext(FULL_REMATCH_PA, Boolean.class));
        if (softDeleteEntities.containsKey(entity) && Boolean.TRUE.equals(softDeleteEntities.get(entity))) {
            masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore, inactive);
        } else {
            if (!Boolean.TRUE.equals(configuration.getNeedReplace()) && !isEntityMatchRematch) {
                masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore, active);
            }
        }
        if (masterTable == null || masterTable.getExtracts().isEmpty()) {
            log.info("There has been no master table for this data collection. Creating a new one. entity is: {}",
                    configuration.getMainEntity());
        } else {
            inputMasterTableName = masterTable.getName();
        }
        log.info("Set inputMasterTableName=" + inputMasterTableName);

        systemBatchTable = dataCollectionProxy.getTable(customerSpace.toString(), systemBatchStore, inactive);
        if (systemBatchTable != null) {
            systemBatchTableName = systemBatchTable.getName();
        }
        log.info("Set system batch name=" + systemBatchTableName);
    }

    TransformationStepConfig softDelete(int mergeSoftDeleteStep, int mergeStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_SOFT_DELETE_TXFMR);
        step.setInputSteps(Arrays.asList(mergeSoftDeleteStep, mergeStep));
        setTargetTable(step, batchStoreTablePrefix);
        SoftDeleteConfig softDeleteConfig = new SoftDeleteConfig();
        softDeleteConfig.setDeleteSourceIdx(0);
        softDeleteConfig.setIdColumn(InterfaceName.AccountId.name());
        step.setConfiguration(appendEngineConf(softDeleteConfig, lightEngineConfig()));
        return step;
    }

    TransformationStepConfig softDelete(int mergeSoftDeleteStep, String inputMasterTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_SOFT_DELETE_TXFMR);
        step.setInputSteps(Collections.singletonList(mergeSoftDeleteStep));
        setTargetTable(step, batchStoreTablePrefix);
        if (StringUtils.isNotBlank(inputMasterTableName)) {
            Table masterTable = metadataProxy.getTable(customerSpace.toString(), inputMasterTableName);
            if (masterTable != null && !masterTable.getExtracts().isEmpty()) {
                log.info("Add masterTable=" + inputMasterTableName);
                addBaseTables(step, inputMasterTableName);
            }
        } else {
            throw new IllegalArgumentException("The master table is empty for soft delete!");
        }
        SoftDeleteConfig softDeleteConfig = new SoftDeleteConfig();
        softDeleteConfig.setDeleteSourceIdx(0);
        softDeleteConfig.setIdColumn(InterfaceName.AccountId.name());
        step.setConfiguration(appendEngineConf(softDeleteConfig, lightEngineConfig()));
        return step;
    }

    TransformationStepConfig upsertMaster(boolean entityMatch, int mergeStep, boolean setTarget) {
        TransformationStepConfig step = new TransformationStepConfig();
        setupMasterTable(step, null);
        step.setInputSteps(Collections.singletonList(mergeStep));
        step.setTransformer(TRANSFORMER_UPSERT_TXMFR);
        if (setTarget) {
            setTargetTable(step, batchStoreTablePrefix);
        }
        UpsertConfig config = getUpsertConfig(entityMatch, true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    TransformationStepConfig upsertSystemBatch(int mergeStep, boolean setTarget) {
        TransformationStepConfig step = new TransformationStepConfig();
        String batchTemplateName = setupSystemBatchTable(step);
        step.setInputSteps(Collections.singletonList(mergeStep));
        step.setTransformer(TRANSFORMER_UPSERT_TXMFR);
        if (setTarget) {
            setTargetTable(step, systemBatchStoreTablePrefix);
        }
        UpsertConfig config = getUpsertConfig(true, true);
        config.setAddInputSystemBatch(true);
        config.setBatchTemplateName(batchTemplateName);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private String setupSystemBatchTable(TransformationStepConfig step) {
        if (StringUtils.isNotBlank(systemBatchTableName)) {
            Table systemBatchTable = metadataProxy.getTable(customerSpace.toString(), systemBatchTableName);
            if (systemBatchTable != null && !systemBatchTable.getExtracts().isEmpty()) {
                log.info("Add systemBatchTable=" + systemBatchTableName);
                addBaseTables(step, systemBatchTableName);
            }
        } else if (StringUtils.isNotBlank(inputMasterTableName)) {
            Table masterTable = metadataProxy.getTable(customerSpace.toString(), inputMasterTableName);
            if (masterTable != null && !masterTable.getExtracts().isEmpty()) {
                log.info("Add masterTable=" + inputMasterTableName);
                addBaseTables(step, inputMasterTableName);
            }
            return SystemBatchTemplateName.Other.name();
        }
        return null;
    }

    TransformationStepConfig mergeSystemBatch(int mergeStep, boolean setTarget) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(mergeStep));
        step.setTransformer(TRANSFORMER_MERGE_SYSTEM_BATCH_TXMFR);
        if (setTarget) {
            setTargetTable(step, batchStoreTablePrefix);
        }
        MergeSystemBatchConfig config = getMergeSystemBatchConfig();
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private MergeSystemBatchConfig getMergeSystemBatchConfig() {
        MergeSystemBatchConfig config = new MergeSystemBatchConfig();
        config.setNotOverwriteByNull(true);
        config.setJoinKey(InterfaceName.EntityId.name());
        List<String> templates = new ArrayList<>();
        templates.add(SystemBatchTemplateName.Other.name());
        templates.add(SystemBatchTemplateName.Embedded.name());
        templates.addAll(templatesInOrder);
        config.setTemplates(templates);
        return config;
    }

    TransformationStepConfig upsertMaster(boolean entityMatch, String matchedTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        setupMasterTable(step, matchedTable);
        step.setTransformer(TRANSFORMER_UPSERT_TXMFR);
        setTargetTable(step, batchStoreTablePrefix);
        UpsertConfig config = getUpsertConfig(entityMatch, false);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private UpsertConfig getUpsertConfig(boolean entityMatch, boolean switchSides) {
        UpsertConfig config = new UpsertConfig();
        config.setColsFromLhs(Collections.singletonList(InterfaceName.CDLCreatedTime.name()));
        config.setNotOverwriteByNull(true);
        if (entityMatch) {
            config.setJoinKey(InterfaceName.EntityId.name());
        } else {
            config.setJoinKey(batchStorePrimaryKey);
        }
        config.setSwitchSides(switchSides);
        return config;
    }

    TransformationStepConfig diff(int newImports, int newMaster) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(newImports, newMaster));
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_DELTA);
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(InterfaceName.Id.name());
        config.setMasterIdField(batchStorePrimaryKey);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        setTargetTable(step, diffTablePrefix);
        return step;
    }

    TransformationStepConfig diff(String newImports, int newMaster) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(newMaster));
        addBaseTables(step, newImports);
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_DELTA);
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setInputLast(true);
        config.setSrcIdField(InterfaceName.Id.name());
        config.setMasterIdField(batchStorePrimaryKey);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        setTargetTable(step, diffTablePrefix);
        return step;
    }

    private void setupMasterTable(TransformationStepConfig step, String inputTable) {
        if (StringUtils.isNotBlank(inputMasterTableName)) {
            Table masterTable = metadataProxy.getTable(customerSpace.toString(), inputMasterTableName);
            if (masterTable != null && !masterTable.getExtracts().isEmpty()) {
                log.info("Add masterTable=" + inputMasterTableName);
                addBaseTables(step, inputMasterTableName);
            }
        }
        if (StringUtils.isNotBlank(inputTable)) {
            log.info("Add inputTable=" + inputTable);
            addBaseTables(step, inputTable);
        }
    }

    /*
     * get the union of all input table columns
     */
    Set<String> getInputTableColumnNames() {
        return getTableColumnNames(inputTableNames.toArray(new String[0]));
    }

    Set<String> getInputTableColumnNames(int tableIdx) {
        String tableName = inputTableNames.get(tableIdx);
        return getTableColumnNames(tableName);
    }

    /**
     * Retrieve all system IDs for target entity of current tenant (sorted by
     * system priority from high to low)
     *
     * @param entity
     *            target entity
     * @return non-null list of system IDs
     */
    protected List<String> getSystemIds(BusinessEntity entity) {
        Map<String, List<String>> systemIdMap = configuration.getSystemIdMap();
        if (MapUtils.isEmpty(systemIdMap)) {
            return Collections.emptyList();
        }
        return systemIdMap.getOrDefault(entity.name(), Collections.emptyList());
    }

    /**
     * Retrieve default system ID for target entity of current tenant. Return
     * null if default system is not setup.
     *
     * @param entity
     *            target entity
     * @return default system ID
     */
    protected String getDefaultSystemId(BusinessEntity entity) {
        Map<String, String> defaultSystemIdMap = configuration.getDefaultSystemIdMap();
        if (MapUtils.isEmpty(defaultSystemIdMap)) {
            return null;
        }

        return defaultSystemIdMap.get(entity.name());
    }

    protected void exportToDynamo(String tableName, String partitionKey, String sortKey) {
        String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), tableName);
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(tableName);
        config.setInputPath(PathUtils.toAvroGlob(inputPath));
        config.setPartitionKey(partitionKey);
        if (StringUtils.isNotBlank(sortKey)) {
            config.setSortKey(sortKey);
        }
        addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }

    protected void enrichSystemBatchTableSchema(Table table) {
    }

    protected void enrichTableSchema(Table table) {
    }

    protected String getSystemBatchStoreName() {
        return TableUtils.getFullTableName(systemBatchStoreTablePrefix, pipelineVersion);
    }

    protected String getBatchStoreName() {
        return TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion);
    }

    protected String getDiffTableName() {
        return TableUtils.getFullTableName(diffTablePrefix, pipelineVersion);
    }
}
