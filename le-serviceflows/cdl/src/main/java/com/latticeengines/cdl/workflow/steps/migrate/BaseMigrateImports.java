package com.latticeengines.cdl.workflow.steps.migrate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
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
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.BaseMigrateImportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.MigrateTrackingProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class BaseMigrateImports<T extends BaseMigrateImportStepConfiguration> extends BaseTransformWrapperStep<T> {

    private static Logger log = LoggerFactory.getLogger(BaseMigrateImports.class);

    private static int migrateStep;

    private static final String TRANSFORMER = "EntityMatchImportMigrateTransformer";

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected DataFeedProxy dataFeedProxy;

    @Inject
    protected CDLProxy cdlProxy;

    @Inject
    protected MigrateTrackingProxy migrateTrackingProxy;

    private Table masterTable;

    private TableRoleInCollection batchStore;

    protected CustomerSpace customerSpace;

    protected Table templateTable;

    protected S3ImportSystem importSystem;

    protected ImportMigrateTracking importMigrateTracking;

    protected abstract String getTargetTablePrefix();

    protected abstract TableRoleInCollection getBatchStore();

    protected abstract Map<String, String> getRenameMap();

    protected abstract Map<String, String> getDuplicateMap();

    protected abstract String getTaskId();

    protected abstract void updateMigrateTracking(Long migratedCounts, List<String> dataTables);

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        PipelineTransformationRequest request = generateRequest();
        return transformationProxy.getWorkflowConf(configuration.getCustomerSpace().toString(), request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String migratedImportTableName = TableUtils.getFullTableName(getTargetTablePrefix(), pipelineVersion);
        Table migratedImportTable = metadataProxy.getTable(customerSpace.toString(), migratedImportTableName);
        Long importCounts = getTableDataLines(migratedImportTable);
        List<String> dataTables = dataFeedProxy.registerExtracts(customerSpace.toString(), getTaskId(),
                templateTable.getName(), migratedImportTable.getExtracts());
        updateMigrateTracking(importCounts, dataTables);
    }

    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        importMigrateTracking = migrateTrackingProxy.getMigrateTracking(customerSpace.toString(),
                configuration.getMigrateTrackingPid());
        if (importMigrateTracking == null || importMigrateTracking.getReport() == null) {
            throw new RuntimeException("Migrate Tracking record is not properly createc!");
        }
        String taskUniqueId = getTaskId();
        String systemName = getStringValueFromContext(PRIMARY_IMPORT_SYSTEM);
        if (StringUtils.isEmpty(taskUniqueId)) {
            throw new RuntimeException("Cannot find the target datafeed task for Account migrate!");
        }
        if (StringUtils.isEmpty(systemName)) {
            throw new RuntimeException("No ImportSystem for import migration!");
        }
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), taskUniqueId);
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find the dataFeedTask with id: " + taskUniqueId);
        }
        templateTable = dataFeedTask.getImportTemplate();
        if (templateTable == null) {
            throw new RuntimeException("Template is NULL for dataFeedTask: " + taskUniqueId);
        }
        importSystem = cdlProxy.getS3ImportSystem(customerSpace.toString(), systemName);
        if (importSystem == null) {
            throw new RuntimeException("Cannot find ImportSystem with name: " + systemName);
        }
        batchStore = getBatchStore();
        masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore);
        if (masterTable == null) {
            throw new RuntimeException(
                    String.format("master table in collection shouldn't be null when customer space %s, role %s",
                            customerSpace.toString(), batchStore));
        }
    }

    protected PipelineTransformationRequest generateRequest() {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MigrateImportStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);

            migrateStep = 0;

            List<TransformationStepConfig> steps = new ArrayList<>();
            TransformationStepConfig migrate = migrate();
            steps.add(migrate);
            request.setSteps(steps);

            return request;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

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
        config.setDuplicateMap(getDuplicateMap());
        config.setRenameMap(getRenameMap());

        String configStr = appendEngineConf(config, lightEngineConfig());
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(getTargetTablePrefix());

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
