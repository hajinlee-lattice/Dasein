package com.latticeengines.cdl.workflow.steps.migrate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.service.ConvertBatchStoreService;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.EntityMatchImportMigrateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.BaseConvertBatchStoreServiceConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component(ConvertBatchStoreToImport.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertBatchStoreToImport extends BaseTransformWrapperStep<ConvertBatchStoreStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ConvertBatchStoreToImport.class);

    static final String BEAN_NAME = "convertBatchStoreToImport";

    private static final String TRANSFORMER = "EntityMatchImportMigrateTransformer";

    private ConvertBatchStoreService convertBatchStoreService;

    private BaseConvertBatchStoreServiceConfiguration convertServiceConfig;

    private Table templateTable;

    private Table masterTable;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        PipelineTransformationRequest request = generateRequest();
        return transformationProxy.getWorkflowConf(configuration.getCustomerSpace().toString(), request, configuration.getPodId());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void onPostTransformationCompleted() {
        String migratedImportTableName =
                TableUtils.getFullTableName(
                        convertBatchStoreService.getTargetTablePrefix(customerSpace.toString(), convertServiceConfig)
                        , pipelineVersion);
        convertBatchStoreService.setDataTable(migratedImportTableName, customerSpace.toString(),
                templateTable, convertServiceConfig, yarnConfiguration);

        Map<String, String> rematchTables = getObjectFromContext(REMATCH_TABLE_NAME, Map.class);
        if (rematchTables == null) {
            rematchTables = new HashMap<>();
        }
        rematchTables.put(configuration.getEntity().name(), migratedImportTableName);
        log.info("rematchTables : {}, config : {}.", rematchTables, convertServiceConfig.getClass());
        putObjectInContext(REMATCH_TABLE_NAME, rematchTables);
    }

    @SuppressWarnings("unchecked")
    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        convertServiceConfig = configuration.getConvertServiceConfig();
        convertBatchStoreService = ConvertBatchStoreService.getConvertService(convertServiceConfig.getClass());

        templateTable = convertBatchStoreService.verifyTenantStatus(customerSpace.toString(), convertServiceConfig);
        TableRoleInCollection batchStore = convertBatchStoreService.getBatchStore(customerSpace.toString(),
                convertServiceConfig);
        masterTable = convertBatchStoreService.getMasterTable(customerSpace.toString(), batchStore,
                convertServiceConfig);
        if (masterTable == null) {
            throw new RuntimeException(
                    String.format("master table in collection shouldn't be null when customer space %s, role %s",
                            customerSpace.toString(), batchStore));
        }
    }

    private PipelineTransformationRequest generateRequest() {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MigrateImportStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);

            List<TransformationStepConfig> steps = new ArrayList<>();
            TransformationStepConfig migrate = migrate();
            steps.add(migrate);
            request.setSteps(steps);

            return request;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private TransformationStepConfig migrate() {
        TransformationStepConfig step = new TransformationStepConfig();

        List<String> sourceNames = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();
        String masterName = masterTable.getName();
        SourceTable source = new SourceTable(masterName, customerSpace);

        sourceNames.add(masterName);
        baseTables.put(masterName, source);

        EntityMatchImportMigrateConfig config = new EntityMatchImportMigrateConfig();
        config.setTransformer(TRANSFORMER);
        config.setRetainFields(convertBatchStoreService.getAttributes(customerSpace.toString(), templateTable,
                masterTable, configuration.getDiscardFields(), convertServiceConfig));
        config.setDuplicateMap(convertBatchStoreService.getDuplicateMap(customerSpace.toString(), convertServiceConfig));
        config.setRenameMap(convertBatchStoreService.getRenameMap(customerSpace.toString(), convertServiceConfig));

        String configStr = appendEngineConf(config, lightEngineConfig());
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(convertBatchStoreService.getTargetTablePrefix(customerSpace.toString(), convertServiceConfig));

        step.setBaseSources(sourceNames);
        step.setBaseTables(baseTables);
        step.setTransformer(TRANSFORMER);
        step.setConfiguration(configStr);
        step.setTargetTable(targetTable);

        return step;
    }

}
