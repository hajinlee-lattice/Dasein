package com.latticeengines.cdl.workflow.steps.process;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.MergeActivityMetricsJobConfig;
import com.latticeengines.domain.exposed.util.CategoryUtils;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.MergeActivityMetrics;

@Component("mergeActivityMetricsToEntityStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class MergeActivityMetricsToEntityStep extends RunSparkJob<ActivityStreamSparkStepConfiguration, MergeActivityMetricsJobConfig> {

    private static final Logger log = LoggerFactory.getLogger(MergeActivityMetricsToEntityStep.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private DataCollection.Version inactive;

    @Override
    protected Class<? extends AbstractSparkJob<MergeActivityMetricsJobConfig>> getJobClz() {
        return MergeActivityMetrics.class;
    }

    @Override
    protected MergeActivityMetricsJobConfig configureJob(ActivityStreamSparkStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        List<ActivityMetricsGroup> groups = new ArrayList<>(stepConfiguration.getActivityMetricsGroupMap().values());
        Map<String, List<ActivityMetricsGroup>> mergedTablesMap = new HashMap<>(); // merged table label -> groups to merge
        Set<String> activityMetricsServingEntities = new HashSet<>();
        groups.forEach(group -> {
            activityMetricsServingEntities.add(CategoryUtils.getEntity(group.getCategory()).get(0).getServingStore().name());
            String mergedTableLabel = getMergedLabel(group);
            mergedTablesMap.putIfAbsent(mergedTableLabel, new ArrayList<>());
            mergedTablesMap.get(mergedTableLabel).add(group);
        });

        // for profiling merged tables
        putObjectInContext(ACTIVITY_MERGED_METRICS_SERVING_ENTITIES, activityMetricsServingEntities);

        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, ActivityStoreSparkIOMetadata.Details> detailsMap = new HashMap<>();
        AtomicInteger index = new AtomicInteger();
        List<DataUnit> inputs = new ArrayList<>();
        mergedTablesMap.forEach((mergedTableLabel, groupsToMerge) -> {
            ActivityStoreSparkIOMetadata.Details details = new ActivityStoreSparkIOMetadata.Details();
            details.setStartIdx(index.get());
            details.setLabels(groups.stream().map(ActivityMetricsGroup::getGroupId).collect(Collectors.toList()));
            detailsMap.put(mergedTableLabel, details);
            index.addAndGet(groups.size());

            inputs.addAll(getMetricsGroupsDUs(groupsToMerge));
        });
        inputMetadata.setMetadata(detailsMap);
        MergeActivityMetricsJobConfig config = new MergeActivityMetricsJobConfig();
        config.inputMetadata = inputMetadata;
        config.mergedTableLabels = new ArrayList<>(mergedTablesMap.keySet());
        config.setInput(inputs);

        return config;
    }

    private List<DataUnit> getMetricsGroupsDUs(List<ActivityMetricsGroup> groupsToMerge) {
        List<String> tableNames = groupsToMerge.stream().map(group -> String.format(METRICS_GROUP_TABLE_FORMAT, group.getGroupId())).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(tableNames)) {
            return Collections.emptyList();
        } else {
            return getTableSummariesFromCtxKeys(customerSpace.toString(), tableNames).stream()
                    .map(table -> table.toHdfsDataUnit(null)).collect(Collectors.toList());
        }
    }

    private String getMergedLabel(ActivityMetricsGroup group) {
        BusinessEntity entity = group.getEntity();
        TableRoleInCollection servingEntity = CategoryUtils.getEntity(group.getCategory()).get(0).getServingStore();
        return String.format("%s_%s", entity, servingEntity);
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String outputMetadataStr = result.getOutput();
        log.info("Generated output metadata: {}", outputMetadataStr);
        log.info("Generated {} merged tables", result.getTargets().size());
        Map<String, ActivityStoreSparkIOMetadata.Details> outputMetadata = JsonUtils.deserialize(outputMetadataStr, ActivityStoreSparkIOMetadata.class).getMetadata();
        Map<TableRoleInCollection, Map<String, String>> signatureTableNames = new HashMap<>();
        outputMetadata.forEach((mergedTableLabel, details) -> {
            HdfsDataUnit output = result.getTargets().get(details.getStartIdx());
            String tableCtxName = String.format(MERGED_METRICS_GROUP_TABLE_FORMAT, mergedTableLabel); // entity_servingEntity (Account_WebVisit)
            String tableName = TableUtils.getFullTableName(tableCtxName, HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
            Table mergedTable = toTable(tableName, output);
            if (output.getCount() <= 0) {
                // create dummy record with meaningless accountId, append to mergedDU
                log.warn("Empty metrics found: {}. Append dummy record.", tableName);
                appendDummyRecord(mergedTable);
            }
            metadataProxy.createTable(customerSpace.toString(), tableName, mergedTable);
            TableRoleInCollection servingEntity = getServingEntityInLabel(mergedTableLabel);
            signatureTableNames.putIfAbsent(servingEntity, new HashMap<>());
            signatureTableNames.get(servingEntity).put(getEntityInLabel(mergedTableLabel).name(), tableName);
            putStringValueInContext(tableCtxName, tableName);
        });
        // signature: entity (Account/Contact)
        // role: WebVisitProfile
        signatureTableNames.keySet().forEach(role -> dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames.get(role), role, inactive));
    }

    private void appendDummyRecord(Table targetTable) {
        List<GenericRecord> dummyRecords = createDummyRecord();
        try {
            String targetPath = ExtractUtils.getSingleExtractPath(yarnConfiguration, targetTable);
            log.info("Retrieved extract path: {}", targetPath);
            if (StringUtils.isBlank(targetPath)) {
                throw new FileNotFoundException("Unable to find the path where extract is located");
            }
            AvroUtils.appendToHdfsFile(yarnConfiguration, targetPath, dummyRecords);
        } catch (IOException e) {
            log.error("Merged metrics is empty but Failed to create dummy record for profiling", e.fillInStackTrace());
        }
    }

    private List<GenericRecord> createDummyRecord() {
        String dummySchemaStr = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"" + InterfaceName.AccountId.name() + "\",\"type\":[\"string\",\"null\"]}]}";
        Schema.Parser parser = new Schema.Parser();
        GenericRecord record = new GenericData.Record(parser.parse(dummySchemaStr));
        record.put(InterfaceName.AccountId.name(), DataCloudConstants.ENTITY_ANONYMOUS_ID);
        return Collections.singletonList(record);
    }

    private TableRoleInCollection getServingEntityInLabel(String mergedTableLabels) {
        String[] labels = mergedTableLabels.split("_");
        return TableRoleInCollection.getByName(labels[1]);
    }

    private BusinessEntity getEntityInLabel(String mergedTableLabels) {
        String[] labels = mergedTableLabels.split("_");
        return BusinessEntity.getByName(labels[0]);
    }
}
