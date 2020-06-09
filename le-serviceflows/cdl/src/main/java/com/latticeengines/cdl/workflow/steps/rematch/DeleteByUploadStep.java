package com.latticeengines.cdl.workflow.steps.rematch;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.DeleteByUploadStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component(DeleteByUploadStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeleteByUploadStep extends BaseTransformWrapperStep<DeleteByUploadStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(DeleteByUploadStep.class);

    private static final String CLEANUP_TABLE_PREFIX = "DeleteByFile";

    static final String BEAN_NAME = "deleteByUploadStep";

    private List<Table> masterTables;

    private CustomerSpace customerSpace;

    private List<Action> hardDeleteTableLists;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        intializeConfiguration();
        if (isShortCutMode()) {
            return null;
        }
        hardDeleteTableLists = getListObjectFromContext(HARD_DELETE_ACTIONS, Action.class);
        if (CollectionUtils.isEmpty(hardDeleteTableLists)) {
            return null;
        }
        PipelineTransformationRequest request = generateRequest();
        return transformationProxy.getWorkflowConf(configuration.getCustomerSpace().toString(), request,
                configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        Map<String, String> tableTemplateMap = getMapObjectFromContext(CONSOLIDATE_INPUT_TEMPLATES, String.class,
                String.class);
        List<String> cleanupTableNames = new LinkedList<>();
        masterTables.forEach(masterTable -> {
            String cleanupTablePrefix = CLEANUP_TABLE_PREFIX + "_" + masterTable.getName();
            String cleanupTableName = TableUtils.getFullTableName(cleanupTablePrefix, pipelineVersion);
            cleanupTableNames.add(cleanupTableName);
            // update the tableTemplateMap with new cleanup tables
            if (MapUtils.isNotEmpty(tableTemplateMap)) {
                tableTemplateMap.put(cleanupTableName, tableTemplateMap.get(masterTable.getName()));
            }
        });
        log.info("cleanupTableNames " + cleanupTableNames);
        setDeletedTableName(cleanupTableNames);
        // Put the table template map back into context
        putObjectInContext(CONSOLIDATE_INPUT_TEMPLATES, tableTemplateMap);
    }

    private void intializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        List<String> masterTableNames = getConvertedRematchTableNames();
        if (CollectionUtils.isEmpty(masterTableNames)) {
            throw new RuntimeException(
                    String.format("master tables in metadataTable shouldn't be null for customer space %s",
                            customerSpace.toString()));
        }
        log.info("masterTableNames " + masterTableNames);
        masterTables = new LinkedList<>();
        masterTableNames.forEach(tableName -> {
            masterTables.add(metadataProxy.getTable(customerSpace.toString(), tableName));
        });
    }

    private PipelineTransformationRequest generateRequest() {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("DeleteByUploadStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);

            List<TransformationStepConfig> steps = new ArrayList<>();
            int mergeStep = 0;
            TransformationStepConfig mergeDelete = mergeHardDelete();
            steps.add(mergeDelete);
            // Loop through the master tables to add multiple hard delete steps
            masterTables.forEach(masterTable -> {
                TransformationStepConfig hardDelete = hardDelete(mergeStep, masterTable.getName());
                steps.add(hardDelete);
            });
            log.info(JsonUtils.serialize(steps));
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    TransformationStepConfig mergeHardDelete() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        hardDeleteTableLists.forEach(action -> {
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

    TransformationStepConfig hardDelete(int mergeSoftDeleteStep, String tableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(mergeSoftDeleteStep));
        step.setTransformer(TRANSFORMER_SOFT_DELETE_TXFMR);
        SourceTable source = new SourceTable(tableName, customerSpace);
        List<String> sourceNames = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(CLEANUP_TABLE_PREFIX + "_" + tableName);

        sourceNames.add(tableName);
        baseTables.put(tableName, source);
        SoftDeleteConfig softDeleteConfig = new SoftDeleteConfig();
        softDeleteConfig.setDeleteSourceIdx(0);
        softDeleteConfig.setIdColumn(InterfaceName.AccountId.name());
        step.setBaseSources(sourceNames);
        step.setBaseTables(baseTables);
        step.setConfiguration(appendEngineConf(softDeleteConfig, lightEngineConfig()));
        step.setTargetTable(targetTable);
        return step;
    }

    private List<String> getConvertedRematchTableNames() {
        Map<String, List<String>> rematchTables = getTypedObjectFromContext(REMATCH_TABLE_NAMES,
                new TypeReference<Map<String, List<String>>>() {
                });
        return (rematchTables != null) && rematchTables.get(configuration.getEntity().name()) != null
                ? rematchTables.get(configuration.getEntity().name())
                : null;
    }

    private void setDeletedTableName(List<String> tableNames) {
        Map<String, List<String>> deletedTables = getTypedObjectFromContext(DELETED_TABLE_NAMES,
                new TypeReference<Map<String, List<String>>>() {
                });
        if (deletedTables == null) {
            deletedTables = new HashMap<>();
        }
        if (CollectionUtils.isNotEmpty(tableNames)) {
            deletedTables.put(configuration.getEntity().name(), tableNames);
            putObjectInContext(DELETED_TABLE_NAMES, deletedTables);
            tableNames.forEach(tableName -> {
                addToListInContext(TEMPORARY_CDL_TABLES, tableName, String.class);
            });
        }
    }

    private boolean isShortCutMode() {
        Map<String, List<String>> deletedTables = getTypedObjectFromContext(DELETED_TABLE_NAMES,
                new TypeReference<Map<String, List<String>>>() {
                });
        if (MapUtils.isEmpty(deletedTables)) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(deletedTables.get(configuration.getEntity().name()))) {
            return true;
        }
        return false;
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
}
