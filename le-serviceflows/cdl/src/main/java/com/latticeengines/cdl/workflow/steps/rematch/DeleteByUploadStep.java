package com.latticeengines.cdl.workflow.steps.rematch;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.CleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.DeleteByUploadStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component(DeleteByUploadStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeleteByUploadStep extends BaseTransformWrapperStep<DeleteByUploadStepConfiguration> {

    private static final String CLEANUP_TABLE_PREFIX = "DeleteByFile";

    private static final String TRANSFORMER = "HardDeleteTransformer";

    static final String BEAN_NAME = "deleteByUploadStep";

    private Table masterTable;

    private CustomerSpace customerSpace;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        intializeConfiguration();
        PipelineTransformationRequest request = generateRequest();
        return transformationProxy.getWorkflowConf(configuration.getCustomerSpace().toString(), request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String cleanupTableName = TableUtils.getFullTableName(CLEANUP_TABLE_PREFIX, pipelineVersion);
        setRematchTableName(cleanupTableName);
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
            TransformationStepConfig mergeDelete = concatImports();
            TransformationStepConfig cleanup = cleanup(mergeStep);
            steps.add(mergeDelete);
            steps.add(cleanup);

            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig cleanup(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        BusinessEntity entity = configuration.getEntity();

        List<String> sourceNames = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();
        step.setInputSteps(Collections.singletonList(inputStep));
        String masterName = masterTable.getName();
        SourceTable source = new SourceTable(masterName, customerSpace);

        sourceNames.add(masterName);
        baseTables.put(masterName, source);

        CleanupOperationType type = CleanupOperationType.BYUPLOAD_ID;

        CleanupConfig config = new CleanupConfig();
        config.setBusinessEntity(entity);
        config.setOperationType(type);
        config.setTransformer(TRANSFORMER);
        config.setBaseJoinedColumns(getJoinedColumns(config.getBusinessEntity()));
        config.setDeleteJoinedColumns(getJoinedColumns(config.getBusinessEntity()));

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

    TransformationStepConfig concatImports() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        configuration.getHardDeleteTableSet().forEach(tblName -> addBaseTables(step, tblName));

        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(false);
        config.setJoinKey(null);
        config.setAddTimestamps(false);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    protected String getConvertBatchStoreTableName() {
        Map<String, String> rematchTables = getObjectFromContext(REMATCH_TABLE_NAME, Map.class);
        return (rematchTables != null) && rematchTables.get(configuration.getEntity().name()) != null ?
                rematchTables.get(configuration.getEntity().name()) : null;
    }

    protected void setRematchTableName(String tableName) {
        Map<String, String> rematchTables = getObjectFromContext(REMATCH_TABLE_NAME, Map.class);
        rematchTables.put(configuration.getEntity().name(), tableName);
        putObjectInContext(REMATCH_TABLE_NAME, rematchTables);
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

    private CleanupConfig.JoinedColumns getJoinedColumns(BusinessEntity entity) {
        CleanupConfig.JoinedColumns joinedColumns = new CleanupConfig.JoinedColumns();
        InterfaceName accountId = InterfaceName.AccountId;
        joinedColumns.setAccountId(accountId.name());
        return joinedColumns;
    }
}
