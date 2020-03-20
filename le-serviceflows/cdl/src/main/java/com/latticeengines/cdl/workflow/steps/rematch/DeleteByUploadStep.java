package com.latticeengines.cdl.workflow.steps.rematch;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

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

    private Table masterTable;

    private CustomerSpace customerSpace;

    private List<Action> hardDeleteTableLists;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        intializeConfiguration();
        if (isShortCutMode()) {
            return null;
        }
        hardDeleteTableLists = getListObjectFromContext(HARD_DEELETE_ACTIONS, Action.class);
        if (CollectionUtils.isEmpty(hardDeleteTableLists)) {
            return null;
        }
        PipelineTransformationRequest request = generateRequest();
        return transformationProxy.getWorkflowConf(configuration.getCustomerSpace().toString(), request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String cleanupTableName = TableUtils.getFullTableName(CLEANUP_TABLE_PREFIX, pipelineVersion);
        setDeletedTableName(cleanupTableName);
    }

    private void intializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        String masterTableName = getConvertBatchStoreTableName();
        masterTable = metadataProxy.getTable(customerSpace.toString(), masterTableName);
        if (masterTable == null) {
            throw new RuntimeException(
                    String.format("master table in metadataTable shouldn't be null when customer space %s, tableName " +
                                    "%s",
                            customerSpace.toString(), masterTableName));
        }
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
            TransformationStepConfig hardDelete = hardDelete(mergeStep);
            steps.add(mergeDelete);
            steps.add(hardDelete);
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

    TransformationStepConfig hardDelete(int mergeSoftDeleteStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(mergeSoftDeleteStep));
        step.setTransformer(TRANSFORMER_SOFT_DELETE_TXFMR);
        String masterName = masterTable.getName();
        SourceTable source = new SourceTable(masterName, customerSpace);
        List<String> sourceNames = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(CLEANUP_TABLE_PREFIX);

        sourceNames.add(masterName);
        baseTables.put(masterName, source);
        SoftDeleteConfig softDeleteConfig = new SoftDeleteConfig();
        softDeleteConfig.setDeleteSourceIdx(0);
        softDeleteConfig.setIdColumn(InterfaceName.AccountId.name());
        step.setBaseSources(sourceNames);
        step.setBaseTables(baseTables);
        step.setConfiguration(appendEngineConf(softDeleteConfig, lightEngineConfig()));
        step.setTargetTable(targetTable);
        return step;
    }

    private String getConvertBatchStoreTableName() {
        Map<String, String> rematchTables = getObjectFromContext(REMATCH_TABLE_NAME, Map.class);
        return (rematchTables != null) && rematchTables.get(configuration.getEntity().name()) != null ?
                rematchTables.get(configuration.getEntity().name()) : null;
    }

    private void setDeletedTableName(String tableName) {
        Map<String, String> deletedTables = getObjectFromContext(DELETED_TABLE_NAME, Map.class);
        if (deletedTables == null) {
            deletedTables = new HashMap<>();
        }
        deletedTables.put(configuration.getEntity().name(), tableName);
        putObjectInContext(DELETED_TABLE_NAME, deletedTables);
        addToListInContext(TEMPORARY_CDL_TABLES, tableName, String.class);
    }

    private boolean isShortCutMode() {
        Map<String, String> deletedTables = getObjectFromContext(DELETED_TABLE_NAME, Map.class);
        if (deletedTables == null) {
            return false;
        }
        if (deletedTables.get(configuration.getEntity().name()) != null) {
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
        for (String sourceTableName: sourceTableNames) {
            SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
            baseSources.add(sourceTableName);
            baseTables.put(sourceTableName, sourceTable);
        }
        step.setBaseSources(baseSources);
        step.setBaseTables(baseTables);
    }
}
