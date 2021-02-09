package com.latticeengines.cdl.workflow.steps.legacydelete;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedRawTransaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.LegacyDeleteByUploadActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component(LegacyDeleteByUpload.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class LegacyDeleteByUpload extends BaseTransformWrapperStep<LegacyDeleteStepConfiguration> {

    private static Logger log = LoggerFactory.getLogger(LegacyDeleteByUpload.class);

    static final String BEAN_NAME = "legacyDeleteByUpload";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    private TableRoleInCollection batchStore;
    private TableRoleInCollection systemBatchStore;
    private Table masterTable;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    private String batchStoreTablePrefix;
    private String systemBatchStoreTablePrefix;
    private Table systemMasterTable;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        switch (configuration.getEntity()) {
            case Account:
                systemBatchStore = TableRoleInCollection.SystemAccount;
                break;
            case Contact:
                systemBatchStore = TableRoleInCollection.SystemContact;
                break;
            default:
                throw new UnsupportedOperationException("No system batch store for " + configuration.getEntity().name());
        }
        systemMasterTable = dataCollectionProxy.getTable(customerSpace.toString(), systemBatchStore, active);
        systemBatchStoreTablePrefix = systemBatchStore.name();
        batchStoreTablePrefix = configuration.getEntity().name();
        batchStore = configuration.getEntity().equals(BusinessEntity.Transaction)
                ? ConsolidatedRawTransaction : configuration.getEntity().getBatchStore();
        masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore, active);
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        registerBatchStore();
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getConsolidateRequest();
        if (request == null) {
            return null;
        } else {
            return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
        }
    }

    private PipelineTransformationRequest getConsolidateRequest() {
        Set<Action> actions = initialData();
        log.info("input is {}. stepConfig is {}.", actions, JsonUtils.serialize(configuration));
        if (CollectionUtils.isEmpty(actions)) {
            return null;
        }
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        String requestName = String.format("legacyDelete%s", configuration.getEntity().name());
        request.setName(requestName);

        List<TransformationStepConfig> steps = new ArrayList<>();
        TransformationStepConfig mergeDelete = mergeDelete(actions, getJoinColumn());
        steps.add(mergeDelete);
        int legacyDeleteMergeStep = steps.size() - 1;
        if (hasSystemStore()) {
            TransformationStepConfig legacyDeleteSystemBatchStore = legacyDelete(legacyDeleteMergeStep, systemMasterTable,
                    getJoinColumn(), InterfaceName.EntityId.name());
            steps.add(legacyDeleteSystemBatchStore);
        }
        TransformationStepConfig legacyDelete = legacyDelete(legacyDeleteMergeStep, masterTable, getJoinColumn(), null);
        steps.add(legacyDelete);

        setOutputTablePrefix(steps);

        request.setSteps(steps);

        return request;
    }

    private TransformationStepConfig mergeDelete(Set<Action> deleteActions, String idColumn) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        deleteActions.forEach(action -> {
            LegacyDeleteByUploadActionConfiguration configuration = (LegacyDeleteByUploadActionConfiguration) action.getActionConfiguration();
            addBaseTables(step, configuration.getTableName());
        });
        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(true);
        config.setJoinKey(idColumn);
        config.setAddTimestamps(false);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig legacyDelete(int mergeDeleteStep, Table masterTable, String joinColumn,
                                                String sourceIdColumn) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_SOFT_DELETE_TXFMR);
        if (masterTable != null) {
            log.info("Add masterTable=" + masterTable.getName());
            addBaseTables(step, masterTable.getName());
        } else {
            throw new IllegalArgumentException("The master table is empty for legacy delete!");
        }
        SoftDeleteConfig softDeleteConfig = new SoftDeleteConfig();
        if (mergeDeleteStep >= 0) {
            step.setInputSteps(Collections.singletonList(mergeDeleteStep));
            softDeleteConfig.setDeleteSourceIdx(0);
        }
        softDeleteConfig.setIdColumn(joinColumn);
        if (StringUtils.isNotEmpty(sourceIdColumn)) {
            softDeleteConfig.setSourceIdColumn(sourceIdColumn);
        }
        step.setConfiguration(appendEngineConf(softDeleteConfig, lightEngineConfig()));
        return step;
    }

    private void setOutputTablePrefix(List<TransformationStepConfig> steps) {
        TransformationStepConfig lastStep = steps.get(steps.size() - 1);
        setTargetTable(lastStep, batchStoreTablePrefix);
        if (hasSystemStore()) {
            TransformationStepConfig secondLastStep = steps.get(steps.size() - 2);
            setTargetTable(secondLastStep, systemBatchStoreTablePrefix);
        }
    }

    private boolean hasSystemStore() {
        return systemMasterTable != null;
    }

    private String getJoinColumn() {
        return BusinessEntity.Contact.equals(configuration.getEntity()) ?
                (configuration.isEntityMatchGAEnabled() ?
                        InterfaceName.CustomerContactId.name() : InterfaceName.ContactId.name()) :
                (configuration.isEntityMatchGAEnabled()?
                        InterfaceName.CustomerAccountId.name() : InterfaceName.AccountId.name());
    }

    private void registerBatchStore() {
        Table table = metadataProxy.getTable(customerSpace.toString(), TableUtils.getFullTableName(batchStoreTablePrefix,
                pipelineVersion));
        if (configuration.getEntity().getBatchStore() != null) {
            if (table == null) {
                if (noImport()) {
                    log.error("cannot clean up all batchStore with no import.");
                    throw new IllegalStateException("cannot clean up all batchStore with no import, PA failed");
                }
                log.warn("Did not generate new table for " + batchStore);
                dataCollectionProxy.unlinkTables(customerSpace.toString(), batchStore, inactive);
            } else {
                if (batchStore.equals(BusinessEntity.Account.getBatchStore())) {
                    // if replaced account batch store, need to link dynamo table
                    String oldBatchStoreName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore,
                            active);
                    DynamoDataUnit dataUnit = (DynamoDataUnit) dataUnitProxy.getByNameAndType(customerSpace.toString(),
                            oldBatchStoreName,
                            DataUnit.StorageType.Dynamo);
                    if (dataUnit != null) {
                        dataUnit.setLinkedTable(StringUtils.isBlank(dataUnit.getLinkedTable()) ? //
                                dataUnit.getName() : dataUnit.getLinkedTable());
                        dataUnit.setName(table.getName());
                        dataUnitProxy.create(customerSpace.toString(), dataUnit);
                    }
                }
                enrichTableSchema(table, masterTable);
                dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), batchStore, inactive);
            }
        }
        if (hasSystemStore()) {
            table = metadataProxy.getTable(customerSpace.toString(), TableUtils.getFullTableName(systemBatchStoreTablePrefix, pipelineVersion));
            if (table == null) {
                log.warn("Did not generate new table for " + systemBatchStore);
                dataCollectionProxy.unlinkTables(customerSpace.toString(), systemBatchStore, inactive);
            } else {
                enrichTableSchema(table, systemMasterTable);
                dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), systemBatchStore, inactive);
            }
        }
    }

    private Set<Action> initialData() {
        switch (configuration.getEntity()) {
            case Account:
                return getSetObjectFromContext(ACCOUNT_LEGACY_DELTE_BYUOLOAD_ACTIONS, Action.class);
            case Contact:
                return getSetObjectFromContext(CONTACT_LEGACY_DELTE_BYUOLOAD_ACTIONS, Action.class);
            default:
                return null;
        }
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

    private boolean noImport() {
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        return MapUtils.isEmpty(entityImportsMap) || !entityImportsMap.containsKey(configuration.getEntity());
    }
}
