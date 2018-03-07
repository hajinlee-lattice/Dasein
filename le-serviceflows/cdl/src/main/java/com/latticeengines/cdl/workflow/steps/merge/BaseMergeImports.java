package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateReportConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class BaseMergeImports<T extends BaseProcessEntityStepConfiguration>
        extends BaseTransformWrapperStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseMergeImports.class);

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    protected CustomerSpace customerSpace;
    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    protected BusinessEntity entity;
    protected TableRoleInCollection batchStore;
    protected String batchStoreTablePrefix;
    protected String mergedBatchStoreName;
    protected String diffTablePrefix;
    protected String diffReportTablePrefix;
    protected String batchStorePrimaryKey;
    protected List<String> batchStoreSortKeys;

    protected List<String> inputTableNames = new ArrayList<>();
    protected Table masterTable;

    @Value("${dataplatform.queue.scheme}")
    protected String queueScheme;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        generateDiffReport();
    }

    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        entity = configuration.getMainEntity();
        batchStore = entity.getBatchStore();
        if (batchStore != null) {
            batchStoreTablePrefix = entity.name();
            batchStorePrimaryKey = batchStore.getPrimaryKey().name();
            batchStoreSortKeys = batchStore.getForeignKeysAsStringList();
            mergedBatchStoreName = batchStore.name() + "_Merged";
        }
        diffTablePrefix = entity.name() + "_Diff";
        diffReportTablePrefix = entity.name() + "_DiffReport";

        @SuppressWarnings("rawtypes")
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);

        List<DataFeedImport> imports = JsonUtils.convertList(entityImportsMap.get(entity), DataFeedImport.class);
        if (CollectionUtils.isNotEmpty(imports)) {
            List<Table> inputTables = imports.stream().map(DataFeedImport::getDataTable).collect(Collectors.toList());

            if (inputTables == null || inputTables.isEmpty()) {
                throw new RuntimeException("There is no input tables to consolidate.");
            }
            Collections.reverse(inputTables);
            inputTables.sort(Comparator.comparing((Table t) -> t.getLastModifiedKey() == null ? -1
                    : t.getLastModifiedKey().getLastModifiedTimestamp() == null ? -1
                            : t.getLastModifiedKey().getLastModifiedTimestamp())
                    .reversed());
            for (Table table : inputTables) {
                inputTableNames.add(table.getName());
            }
        }
    }

    protected TransformationStepConfig reportDiff(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        step.setTransformer("ConsolidateReporter");
        ConsolidateReportConfig config = new ConsolidateReportConfig();
        config.setEntity(entity);
        String configStr = appendEngineConf(config, lightEngineConfig());
        step.setConfiguration(configStr);
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(diffReportTablePrefix);
        step.setTargetTable(targetTable);
        return step;

    }

    protected TransformationStepConfig mergeInputs(boolean useTargetTable, boolean isDedupeSource, boolean mergeOnly) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<String> baseSources = inputTableNames;
        step.setBaseSources(baseSources);

        Map<String, SourceTable> baseTables = new HashMap<>();
        for (String inputTableName : inputTableNames) {
            baseTables.put(inputTableName, new SourceTable(inputTableName, customerSpace));
        }
        step.setBaseTables(baseTables);
        step.setTransformer("consolidateDataTransformer");
        step.setConfiguration(getConsolidateDataConfig(isDedupeSource, true, mergeOnly));
        if (useTargetTable) {
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(mergedBatchStoreName);
            step.setTargetTable(targetTable);
        }
        return step;
    }

    protected String getConsolidateDataConfig(boolean isDedupeSource, boolean addTimettamps, boolean isMergeOnly) {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(InterfaceName.Id.name());
        config.setMasterIdField(batchStorePrimaryKey);
        config.setDedupeSource(isDedupeSource);
        config.setMergeOnly(isMergeOnly);
        config.setAddTimestamps(addTimettamps);
        return appendEngineConf(config, lightEngineConfig());
    }

    protected void generateDiffReport() {
        Table diffReport = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(diffReportTablePrefix, pipelineVersion));
        if (diffReport != null) {
            ObjectNode report = getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                    ObjectNode.class);
            updateReportPayload(report, diffReport);
            putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), report);
            metadataProxy.deleteTable(customerSpace.toString(), diffReport.getName());
        } else {
            log.warn("Didn't find ConsolidationSummaryReport table for entity " + entity);
        }
    }

    protected void updateReportPayload(ObjectNode report, Table diffReport) {
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            String path = diffReport.getExtracts().get(0).getPath();
            Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, path);
            ObjectNode newItem = (ObjectNode) om
                    .readTree(records.next().get(InterfaceName.ConsolidateReport.name()).toString());

            JsonNode entitiesSummaryNode = report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
            if (entitiesSummaryNode == null) {
                entitiesSummaryNode = report.putObject(ReportPurpose.ENTITIES_SUMMARY.getKey());
            }
            JsonNode entityNode = entitiesSummaryNode.get(entity.name());
            if (entityNode == null) {
                entityNode = ((ObjectNode) entitiesSummaryNode).putObject(entity.name());
            }
            JsonNode consolidateSummaryNode = entityNode.get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            if (consolidateSummaryNode == null) {
                consolidateSummaryNode = ((ObjectNode) entityNode)
                        .putObject(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            }
            ((ObjectNode) consolidateSummaryNode).setAll((ObjectNode) newItem.get(entity.name()));

        } catch (Exception e) {
            throw new RuntimeException("Fail to update report payload", e);
        }
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getConsolidateRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(entity, key, value, clz);
    }

    protected <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }

    protected abstract PipelineTransformationRequest getConsolidateRequest();
}
