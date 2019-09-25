package com.latticeengines.cdl.workflow.steps.rematch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.service.ConvertBatchStoreService;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.EntityMatchImportMigrateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.BaseConvertBatchStoreServiceConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertBatchStoreToDataTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component(ConvertBatchStoreToDataTable.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertBatchStoreToDataTable extends BaseTransformWrapperStep<ConvertBatchStoreToDataTableConfiguration> {

    static final String BEAN_NAME = "convertBatchStoreToTable";

    private static final String TRANSFORMER = "EntityReMatchImportMigrateTransformer";

    @Inject
    protected DataFeedProxy dataFeedProxy;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    private ConvertBatchStoreService convertBatchStoreService;

    private BaseConvertBatchStoreServiceConfiguration convertServiceConfig;

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
        Map<String, String> rematchTables = getObjectFromContext(REMATCH_TABLE_NAME, Map.class);
        if (rematchTables == null) {
            rematchTables = new HashMap<>();
        }
        rematchTables.put(configuration.getEntity().name(), migratedImportTableName);
        putObjectInContext(REMATCH_TABLE_NAME, rematchTables);
    }

    @SuppressWarnings("unchecked")
    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        convertServiceConfig = configuration.getConvertServiceConfig();
        convertBatchStoreService = ConvertBatchStoreService.getConvertService(convertServiceConfig.getClass());

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
        Set<Attribute> attributeSet = getAllTemplateAttribute();

        EntityMatchImportMigrateConfig config = new EntityMatchImportMigrateConfig();
        config.setTransformer(TRANSFORMER);
        config.setRetainFields(attributeSet.stream().map(Attribute::getName).collect(Collectors.toList()));

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

    private Set<Attribute> getAllTemplateAttribute() {
        List<DataFeedTask> dataFeedTaskList =
                dataFeedProxy.getDataFeedTaskWithSameEntity(configuration.getCustomerSpace().toString(),
                        configuration.getEntity().name());
        if (CollectionUtils.isEmpty(dataFeedTaskList)) {
            throw new RuntimeException(String.format("Cannot find the dataFeedTask in tenant %s, entity: %s.: " + configuration.getCustomerSpace(), configuration.getEntity()));
        }
        Set<Attribute> attributeSet = new HashSet<>();
        for (DataFeedTask dataFeedTask : dataFeedTaskList) {
            Table templateTable = dataFeedTask.getImportTemplate();
            attributeSet.addAll(templateTable.getAttributes());
        }
        return attributeSet;
    }
}
