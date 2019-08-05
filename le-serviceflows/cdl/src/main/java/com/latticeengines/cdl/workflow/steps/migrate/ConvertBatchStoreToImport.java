package com.latticeengines.cdl.workflow.steps.migrate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.service.ConvertBatchStoreService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.EntityMatchImportMigrateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.BaseConvertBatchStoreServiceConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component(ConvertBatchStoreToImport.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertBatchStoreToImport extends BaseTransformWrapperStep<ConvertBatchStoreStepConfiguration> {

    static final String BEAN_NAME = "convertBatchStoreToImport";

//    private static int migrateStep;

    private static final String TRANSFORMER = "EntityMatchImportMigrateTransformer";

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected DataFeedProxy dataFeedProxy;

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
        Table migratedImportTable = metadataProxy.getTable(customerSpace.toString(), migratedImportTableName);
        Long importCounts = getTableDataLines(migratedImportTable);
        List<String> dataTables = dataFeedProxy.registerExtracts(customerSpace.toString(),
                convertBatchStoreService.getOutputDataFeedTaskId(customerSpace.toString(), convertServiceConfig),
                templateTable.getName(), migratedImportTable.getExtracts());
        convertBatchStoreService.updateConvertResult(customerSpace.toString(), convertServiceConfig, importCounts,
                dataTables);
    }

    @SuppressWarnings("unchecked")
    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        convertServiceConfig = configuration.getConvertServiceConfig();
        convertBatchStoreService = ConvertBatchStoreService.getConvertService(convertServiceConfig.getClass());

        String taskUniqueId = convertBatchStoreService.getOutputDataFeedTaskId(customerSpace.toString(), convertServiceConfig);
//        String systemName = getStringValueFromContext(PRIMARY_IMPORT_SYSTEM);
        if (StringUtils.isEmpty(taskUniqueId)) {
            throw new RuntimeException("Cannot find the target datafeed task for Account migrate!");
        }
//        if (StringUtils.isEmpty(systemName)) {
//            throw new RuntimeException("No ImportSystem for import migration!");
//        }
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), taskUniqueId);
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find the dataFeedTask with id: " + taskUniqueId);
        }
        templateTable = dataFeedTask.getImportTemplate();
        if (templateTable == null) {
            throw new RuntimeException("Template is NULL for dataFeedTask: " + taskUniqueId);
        }
//        importSystem = cdlProxy.getS3ImportSystem(customerSpace.toString(), systemName);
//        if (importSystem == null) {
//            throw new RuntimeException("Cannot find ImportSystem with name: " + systemName);
//        }
        TableRoleInCollection batchStore = convertBatchStoreService.getBatchStore(customerSpace.toString(),
                convertServiceConfig);
        masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore);
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

//            migrateStep = 0;

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
        config.setRetainFields(templateTable.getAttributes().stream().map(Attribute::getName).collect(Collectors.toList()));
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

    private Long getTableDataLines(Table table) {
        if (table == null || table.getExtracts() == null) {
            return 0L;
        }
        Long lines = 0L;
        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            if (!extract.getPath().endsWith("avro")) {
                paths.add(extract.getPath() + "/*.avro");
            } else {
                paths.add(extract.getPath());
            }
        }
        for (String path : paths) {
            lines += AvroUtils.count(yarnConfiguration, path);
        }
        return lines;
    }
}
