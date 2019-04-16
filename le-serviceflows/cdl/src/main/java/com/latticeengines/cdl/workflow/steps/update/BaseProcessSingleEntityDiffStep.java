package com.latticeengines.cdl.workflow.steps.update;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class BaseProcessSingleEntityDiffStep<T extends BaseProcessEntityStepConfiguration>
        extends BaseProcessDiffStep<T> {

    protected BusinessEntity entity;

    String diffTableName;

    private String sortedTablePrefix;
    private String profileTableName;
    private TableRoleInCollection servingStore;
    private String servingStorePrimaryKey;
    private List<String> servingStoreSortKeys;

    @Inject
    protected MetadataProxy metadataProxy;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        String diffServingTableName = TableUtils.getFullTableName(sortedTablePrefix, pipelineVersion);
        Map<TableRoleInCollection, String> processedTableNames = getMapObjectFromContext(PROCESSED_DIFF_TABLES, //
                TableRoleInCollection.class, String.class);
        if (processedTableNames == null) {
            processedTableNames = new HashMap<>();
        }
        processedTableNames.put(entity.getServingStore(), diffServingTableName);
        putObjectInContext(PROCESSED_DIFF_TABLES, processedTableNames);
        addToListInContext(TEMPORARY_CDL_TABLES, diffServingTableName, String.class);
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();

        entity = configuration.getMainEntity();
        servingStore = entity.getServingStore();
        if (servingStore != null) {
            servingStorePrimaryKey = servingStore.getPrimaryKey().name();
            servingStoreSortKeys = servingStore.getForeignKeysAsStringList();
        }

        Map<BusinessEntity, String> diffTableNames = getMapObjectFromContext(ENTITY_DIFF_TABLES, BusinessEntity.class,
                String.class);
        diffTableName = diffTableNames.get(entity);

        if (profileTableRole() != null) {
            profileTableName = dataCollectionProxy.getTableName(customerSpace.toString(), profileTableRole(), inactive);
        }
        sortedTablePrefix = entity.name() + "_Diff_Sorted";
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getTransformRequest();
        return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
    }

    protected TransformationStepConfig bucket(boolean heavyEngine) {
        return bucket(-1, heavyEngine);
    }

    protected TransformationStepConfig bucket(int inputStep, boolean heavyEngine) {
        TransformationStepConfig step = new TransformationStepConfig();
        addBaseTables(step, profileTableName);
        if (inputStep < 0) {
            addBaseTables(step, diffTableName);
        } else {
            step.setInputSteps(Collections.singletonList(inputStep));
        }
        step.setTransformer(TRANSFORMER_BUCKETER);
        String confStr = heavyEngine ? emptyStepConfig(heavyEngineConfig()) : emptyStepConfig(lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    protected TransformationStepConfig retainFields(int previousStep) {
        return retainFields(previousStep, sortedTablePrefix, servingStorePrimaryKey, true, servingStore);
    }

    protected TransformationStepConfig sort(int diffStep, int partitions) {
        TransformationStepConfig step = new TransformationStepConfig();
        if (diffStep < 0) {
            addBaseTables(step, diffTableName);
        } else {
            step.setInputSteps(Collections.singletonList(diffStep));
        }
        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(sortedTablePrefix);
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(true);
        step.setTargetTable(targetTable);

        SorterConfig config = new SorterConfig();
        config.setPartitions(partitions);
        String sortingKey = servingStorePrimaryKey;
        if (!servingStoreSortKeys.isEmpty()) {
            sortingKey = servingStoreSortKeys.get(0);
        }
        config.setSortingField(sortingKey);
        config.setCompressResult(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    protected abstract TableRoleInCollection profileTableRole();

}
