package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

public abstract class BaseSingleEntityMergeImports<T extends BaseProcessEntityStepConfiguration>
        extends BaseMergeImports<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseSingleEntityMergeImports.class);

    private String inputMasterTableName;
    protected String diffTableName;
    protected Table masterTable;

    private List<BusinessEntity> businessEntities = Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact,
            BusinessEntity.Product, BusinessEntity.Transaction);

    @Override
    protected void onPostTransformationCompleted() {
        registerBatchStore();
        generateDiffReport();

        diffTableName = getDiffTableName();
        updateEntityValueMapInContext(ENTITY_DIFF_TABLES, diffTableName, String.class);
        addToListInContext(TEMPORARY_CDL_TABLES, diffTableName, String.class);

        if (hasSchemaChange()) {
            List<BusinessEntity> entityList = getListObjectFromContext(ENTITIES_WITH_SCHEMA_CHANGE, BusinessEntity.class);
            if (entityList == null) {
                entityList = new ArrayList<>();
            }
            entityList.add(entity);
            putObjectInContext(ENTITIES_WITH_SCHEMA_CHANGE, entityList);
        }
    }


    private void registerBatchStore() {
        Table table = metadataProxy.getTable(customerSpace.toString(), getBatchStoreName());
        if (entity.getBatchStore() != null) {
            if (table == null) {
                throw new IllegalStateException("Did not generate new table for " + batchStore);
            }
            isDataQuotaLimit(table);
            enrichTableSchema(table);
            dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), batchStore, inactive);
        }
    }

    protected void isDataQuotaLimit(Table table) {
        if (businessEntities.contains(configuration.getMainEntity())) {
            List<Extract> extracts = table.getExtracts();
            if (!CollectionUtils.isEmpty(extracts)) {
                Long dataCount = 0L;
                for (Extract extract : extracts) {
                    dataCount = dataCount + extract.getProcessedRecords();
                    log.info("stored " + configuration.getMainEntity() + " data is " + dataCount);
                    if (configuration.getDataQuotaLimit() < dataCount)
                        throw new IllegalStateException("the " + configuration.getMainEntity() + " data quota limit is " + configuration.getDataQuotaLimit() +
                                ", The data you uploaded has exceeded the limit.");
                }
                log.info("stored data is " + dataCount + ", the " + configuration.getMainEntity() + "data limit is " + configuration.getDataQuotaLimit());
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
            log.info("These attributes are in the active batch store but not the inactive one: " + diffAttrs1);
            log.info("These attributes are in the inactive batch store but not the active one: " + diffAttrs2);
            return true;
        }

        return false;
    }

    protected void initializeConfiguration() {
        super.initializeConfiguration();
        masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore, active);
        if (masterTable == null || masterTable.getExtracts().isEmpty()) {
            log.info("There has been no master table for this data collection. Creating a new one");
        } else {
            inputMasterTableName = masterTable.getName();
        }
        log.info("Set inputMasterTableName=" + inputMasterTableName);
    }

    TransformationStepConfig mergeMaster(int mergeStep) {
        TargetTable targetTable;
        TransformationStepConfig step = new TransformationStepConfig();
        setupMasterTable(step);
        step.setInputSteps(Collections.singletonList(mergeStep));
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_DATA);
        step.setConfiguration(getConsolidateDataConfig(false, false, false, true));

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(batchStoreTablePrefix);
        step.setTargetTable(targetTable);
        return step;
    }

    TransformationStepConfig diff(int mergeStep, int upsertMasterStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(mergeStep, upsertMasterStep));
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_DELTA);
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(InterfaceName.Id.name());
        config.setMasterIdField(batchStorePrimaryKey);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(diffTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

    void setupMasterTable(TransformationStepConfig step) {
        List<String> baseSources;
        Map<String, SourceTable> baseTables;
        if (StringUtils.isNotBlank(inputMasterTableName)) {
            Table masterTable = metadataProxy.getTable(customerSpace.toString(), inputMasterTableName);
            if (masterTable != null && !masterTable.getExtracts().isEmpty()) {
                baseSources = Collections.singletonList(inputMasterTableName);
                baseTables = new HashMap<>();
                SourceTable sourceMasterTable = new SourceTable(inputMasterTableName, customerSpace);
                baseTables.put(inputMasterTableName, sourceMasterTable);
                step.setBaseSources(baseSources);
                step.setBaseTables(baseTables);
            }
        }
    }

    MatchInput getBaseMatchInput() {
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setExcludePublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(false);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(false);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);

        return matchInput;
    }

    /**
     * Add all LDC match keys to the key map only if they are provided in the import
     * file.
     *
     * @param cols
     *            columns in the import file
     * @param keyMap
     *            key map that will be used for bulk match
     */
    void addLDCMatchKeysIfExist(Set<String> cols, Map<MatchKey, List<String>> keyMap) {
        addMatchKeyIfExists(cols, keyMap, MatchKey.Domain, InterfaceName.Website.name());
        addMatchKeyIfExists(cols, keyMap, MatchKey.DUNS, InterfaceName.DUNS.name());

        addMatchKeyIfExists(cols, keyMap, MatchKey.Name, InterfaceName.CompanyName.name());
        addMatchKeyIfExists(cols, keyMap, MatchKey.City, InterfaceName.City.name());
        addMatchKeyIfExists(cols, keyMap, MatchKey.State, InterfaceName.State.name());
        addMatchKeyIfExists(cols, keyMap, MatchKey.Country, InterfaceName.Country.name());

        addMatchKeyIfExists(cols, keyMap, MatchKey.PhoneNumber, InterfaceName.PhoneNumber.name());
        addMatchKeyIfExists(cols, keyMap, MatchKey.Zipcode, InterfaceName.PostalCode.name());
    }

    /*
     * if columnName exists in cols (columns of import file), add columnName to the
     * list in the keyMap (for the specified match key). a new list will be created
     * if not exist.
     */
    void addMatchKeyIfExists(Set<String> cols, Map<MatchKey, List<String>> keyMap, MatchKey key, String columnName) {
        if (cols.contains(columnName)) {
            keyMap.putIfAbsent(key, new ArrayList<>());
            keyMap.get(key).add(columnName);
        }
    }

    Set<String> getInputTableColumnNames(int tableIdx) {
        String tableName = inputTableNames.get(tableIdx);
        return metadataProxy.getTableColumns(customerSpace.toString(), tableName) //
                .stream() //
                .map(ColumnMetadata::getAttrName) //
                .collect(Collectors.toSet());
    }

    protected void enrichTableSchema(Table table) {
    }

    protected String getBatchStoreName() {
        return TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion);
    }

    protected String getDiffTableName() {
        return TableUtils.getFullTableName(diffTablePrefix, pipelineVersion);
    }

}
