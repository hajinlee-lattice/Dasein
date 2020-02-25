package com.latticeengines.cdl.workflow.steps.process;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.TemplateUtils;
import com.latticeengines.domain.exposed.StringTemplateConstants;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.MergeActivityMetricsJobConfig;
import com.latticeengines.domain.exposed.util.CategoryUtils;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
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

    @Inject
    private PeriodProxy periodProxy;

    private DataCollection.Version inactive;
    private boolean shortCutMode = false;

    private ConcurrentMap<String, Map<String, DimensionMetadata>> streamMetadataCache;
    private static TypeReference<ConcurrentMap<String, Map<String, DimensionMetadata>>> streamMetadataCacheTypeRef = new TypeReference<ConcurrentMap<String, Map<String, DimensionMetadata>>>() {
    };

    @Override
    protected Class<? extends AbstractSparkJob<MergeActivityMetricsJobConfig>> getJobClz() {
        return MergeActivityMetrics.class;
    }

    @Override
    protected MergeActivityMetricsJobConfig configureJob(ActivityStreamSparkStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        streamMetadataCache = JsonUtils.deserializeByTypeRef(getStringValueFromContext(ACTIVITY_STREAM_METADATA_CACHE), streamMetadataCacheTypeRef);
        Set<String> skippedStreams = getSkippedStreamIds();
        List<ActivityMetricsGroup> groups = stepConfiguration.getActivityMetricsGroupMap().values().stream()
                .filter(g -> !skippedStreams.contains(g.getStream().getStreamId())).collect(Collectors.toList());
        Map<String, List<ActivityMetricsGroup>> mergedTablesMap = new HashMap<>(); // merged table label -> groups to merge
        Set<String> activityMetricsServingEntities = new HashSet<>();
        if (CollectionUtils.isEmpty(groups)) {
            log.info("No groups to merge for tenant {}. Skip merging metrics groups", customerSpace);
            return null;
        }
        groups.forEach(group -> {
            activityMetricsServingEntities.add(CategoryUtils.getEntity(group.getCategory()).get(0).getServingStore().name());
            String mergedTableLabel = getMergedLabel(group);
            mergedTablesMap.putIfAbsent(mergedTableLabel, new ArrayList<>());
            mergedTablesMap.get(mergedTableLabel).add(group);
        });
        // for profiling merged tables
        putObjectInContext(ACTIVITY_MERGED_METRICS_SERVING_ENTITIES, activityMetricsServingEntities);
        Map<String, String> mergedMetricsGroupTableNames = getMapObjectFromContext(MERGED_METRICS_GROUP_TABLE_NAME, String.class, String.class);
        shortCutMode = isShortCutMode(mergedMetricsGroupTableNames);
        if (shortCutMode) {
            Map<TableRoleInCollection, Map<String, String>> signatureTableNames = new HashMap<>();
            for (Map.Entry<String, String> entry : mergedMetricsGroupTableNames.entrySet()) {
                String mergedTableLabel = entry.getKey();
                String tableName = entry.getValue();
                TableRoleInCollection servingEntity = getServingEntityInLabel(mergedTableLabel);
                signatureTableNames.putIfAbsent(servingEntity, new HashMap<>());
                signatureTableNames.get(servingEntity).put(getEntityInLabel(mergedTableLabel).name(), tableName);
            }
            log.info(String.format("Found merge activity metrics tables: %s in context, going thru short-cut mode.", mergedMetricsGroupTableNames.values()));
            signatureTableNames.keySet().forEach(role -> dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames.get(role), role, inactive));
            return null;
        } else {
            ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
            Map<String, ActivityStoreSparkIOMetadata.Details> detailsMap = new HashMap<>();
            AtomicInteger index = new AtomicInteger();
            List<DataUnit> inputs = new ArrayList<>();
            mergedTablesMap.forEach((mergedTableLabel, groupsToMerge) -> {
                ActivityStoreSparkIOMetadata.Details details = new ActivityStoreSparkIOMetadata.Details();
                details.setStartIdx(index.get());
                details.setLabels(groupsToMerge.stream().map(ActivityMetricsGroup::getGroupId).collect(Collectors.toList()));
                detailsMap.put(mergedTableLabel, details);
                index.addAndGet(groupsToMerge.size());

                inputs.addAll(getMetricsGroupsDUs(groupsToMerge));
            });
            inputMetadata.setMetadata(detailsMap);
            MergeActivityMetricsJobConfig config = new MergeActivityMetricsJobConfig();
            config.inputMetadata = inputMetadata;
            config.mergedTableLabels = new ArrayList<>(mergedTablesMap.keySet());
            config.setInput(inputs);
            return config;
        }
    }

    private List<DataUnit> getMetricsGroupsDUs(List<ActivityMetricsGroup> groupsToMerge) {
        Map<String, String> metricsGroupTableNames = getMapObjectFromContext(METRICS_GROUP_TABLE_NAME, String.class, String.class);
        List<String> tableNames = groupsToMerge.stream().map(group -> metricsGroupTableNames.get(group.getGroupId())).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(tableNames)) {
            return Collections.emptyList();
        } else {
            return getTableSummaries(customerSpace.toString(), tableNames).stream()
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
        if (shortCutMode) {
            return;
        }
        String outputMetadataStr = result.getOutput();
        log.info("Generated output metadata: {}", outputMetadataStr);
        log.info("Generated {} merged tables", result.getTargets().size());
        Map<String, ActivityStoreSparkIOMetadata.Details> outputMetadata = JsonUtils.deserialize(outputMetadataStr, ActivityStoreSparkIOMetadata.class).getMetadata();
        Map<TableRoleInCollection, Map<String, String>> signatureTableNames = new HashMap<>();
        Map<String, Table> mergedMetricsGroupTables = new HashMap<>();
        outputMetadata.forEach((mergedTableLabel, details) -> {
            HdfsDataUnit output = result.getTargets().get(details.getStartIdx());
            String tableName = customerSpace.getTenantId() + "_" + NamingUtils.timestamp(TableRoleInCollection.WebVisitProfile.name());
            Table mergedTable = toTable(tableName, output);
            if (output.getCount() <= 0) {
                // create dummy record with meaningless accountId, append to mergedDU
                log.warn("Empty metrics found: {}. Append dummy record.", tableName);
                appendDummyRecord(mergedTable);
            } else {
                enrichActivityAttributes(mergedTable, getEntityInLabel(mergedTableLabel));
            }
            metadataProxy.createTable(customerSpace.toString(), tableName, mergedTable);
            TableRoleInCollection servingEntity = getServingEntityInLabel(mergedTableLabel);
            signatureTableNames.putIfAbsent(servingEntity, new HashMap<>());
            signatureTableNames.get(servingEntity).put(getEntityInLabel(mergedTableLabel).name(), tableName);
            mergedMetricsGroupTables.put(mergedTableLabel, mergedTable);
        });
        // signature: entity (Account/Contact)
        // role: WebVisitProfile
        exportToS3AndAddToContext(mergedMetricsGroupTables, MERGED_METRICS_GROUP_TABLE_NAME);
        signatureTableNames.keySet().forEach(role -> dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames.get(role), role, inactive));
    }

    private void enrichActivityAttributes(Table mergedTable, BusinessEntity entity) {
        String entityIdAttrName = getEntityIdColName(entity);
        mergedTable.getAttributes().stream()
                .filter(attribute -> !entityIdAttrName.equalsIgnoreCase(attribute.getName()))
                .forEach(attr -> {
                    String attrName = attr.getName();
                    List<String> tokens;
                    try {
                        tokens = ActivityMetricsGroupUtils.parseAttrName(attrName);
                    } catch (ParseException e) {
                        throw new IllegalArgumentException("Cannot parse metric attribute " + attrName, e);
                    }
                    String groupId = tokens.get(0);
                    String[] rollupDimVals = tokens.get(1).split("_");
                    String timeRange = tokens.get(2);

                    String evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
                    BusinessCalendar calendar = periodProxy.getBusinessCalendar(customerSpace.toString());
                    TimeFilterTranslator translator = new TimeFilterTranslator(getPeriodStrategies(calendar), evaluationDate);
                    populateAttrDisplayName(attr, groupId, rollupDimVals, timeRange);
                    setAttrEvaluatedDate(attr, timeRange, translator);
                });
    }

    private void populateAttrDisplayName(Attribute attr, String groupId, String[] rollupDimVals, String timeRange) {
        ActivityMetricsGroup group = configuration.getActivityMetricsGroupMap().get(groupId);
        AtlasStream stream = group.getStream();
        String attrName = attr.getName();
        String[] rollupDimNames = group.getRollupDimensions().split(",");
        if (rollupDimNames.length != rollupDimVals.length) {
            throw new IllegalArgumentException(
                    String.format("There are %d dimensions in attribute %s, but only %d was defined in group %s", //
                            rollupDimVals.length, attrName, rollupDimNames.length, groupId));
        }
        Map<String, DimensionMetadata> streamDimMetadata = getStreamMetadataFromCache(stream);
        Map<String, Object> params = createDisplayNameParamMap(rollupDimNames, rollupDimVals, streamDimMetadata, attrName);
        try {
            String timeDesc = ActivityMetricsGroupUtils.timeRangeTmplToDescription(timeRange);
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_TIME_RANGE_TOKEN, timeDesc);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse time range for attribute " + attrName, e);
        }
        String displayNameTmpl = group.getDisplayNameTmpl().getTemplate();
        if (StringUtils.isNotBlank(displayNameTmpl)) {
            try {
                attr.setDisplayName(TemplateUtils.renderByMap(displayNameTmpl, params));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to render display name for attribute " + attrName, e);
            }
        }
    }

    private Map<String, Object> createDisplayNameParamMap(String[] rollupDimNames, String[] rollupDimVals, Map<String, DimensionMetadata> streamDimMetadata, String attrName) {
        Map<String, Object> params = new HashMap<>();
        for (int i = 0; i < rollupDimNames.length; i++) {
            String dimName = rollupDimNames[i];
            String dimVal = rollupDimVals[i];

            Map<String, Object> dimParams = new HashMap<>();
            if (streamDimMetadata.containsKey(dimName)) {
                DimensionMetadata dimMetadata = streamDimMetadata.get(dimName);
                dimParams.putAll(dimMetadata.getDimensionValues().stream() //
                        .filter(row -> dimVal.equalsIgnoreCase(row.get(dimName).toString())) //
                        .findFirst().orElse(new HashMap<>()));
            }
            if (MapUtils.isEmpty(dimParams)) {
                throw new IllegalArgumentException( //
                        String.format("Cannot find dimension metadata for %s=%s, in attribute %s", //
                                dimName, dimVal, attrName));
            }
            params.put(dimName, dimParams);
        }
        return params;
    }

    private Map<String, DimensionMetadata> getStreamMetadataFromCache(AtlasStream stream) {
        Map<String, DimensionMetadata> streamDimMetadata = streamMetadataCache.get(stream.getStreamId());
        if (MapUtils.isEmpty(streamDimMetadata)) {
            throw new IllegalArgumentException( //
                    String.format("Cannot find dimension metadata for stream %s", stream.getStreamId()));
        }
        return streamDimMetadata;
    }

    private void setAttrEvaluatedDate(Attribute attr, String timeRange, TimeFilterTranslator translator) {
        TimeFilter timeFilter = ActivityMetricsGroupUtils.timeRangeTmplToTimeFilter(timeRange);
        if (ComparisonType.EVER.equals(timeFilter.getRelation())) {
            return;
        }
        Pair<Integer, Integer> periodIdRange = translator.translateRange(timeFilter);
        Pair<String, String> dateRange = translator.periodIdRangeToDateRange(timeFilter.getPeriod(), periodIdRange);
        attr.setSecondaryDisplayName(String.format(StringTemplateConstants.ACTIVITY_METRICS_ATTR_SECONDARY_DISPLAYNAME, dateRange.getLeft(), dateRange.getRight()));
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

    private String getEntityIdColName(BusinessEntity entity) {
        switch (entity) {
            case Account:
                return InterfaceName.AccountId.name();
            case Contact:
                return InterfaceName.ContactId.name();
            default:
                throw new UnsupportedOperationException(String.format("%s cannot be entity of activity metrics", entity));
        }
    }

    private Set<String> getSkippedStreamIds() {
        if (!hasKeyInContext(ACTIVITY_STREAMS_SKIP_AGG)) {
            return Collections.emptySet();
        }

        Set<String> skippedStreamIds = getSetObjectFromContext(ACTIVITY_STREAMS_SKIP_AGG, String.class);
        log.info("Stream IDs skipped for metrics processing = {}", skippedStreamIds);
        return skippedStreamIds;
    }

    private List<PeriodStrategy> getPeriodStrategies(BusinessCalendar calendar) {
        Set<String> skippedStreamIds = getSkippedStreamIds();
        Set<String> periods = new HashSet<>();
        configuration.getActivityStreamMap().values().stream()
                .filter(stream -> !skippedStreamIds.contains(stream.getStreamId()))
                .forEach(stream -> periods.addAll(stream.getPeriods()));
        return periods.stream().map(period -> new PeriodStrategy(calendar, PeriodStrategy.Template.fromName(period))).collect(Collectors.toList());
    }
}
