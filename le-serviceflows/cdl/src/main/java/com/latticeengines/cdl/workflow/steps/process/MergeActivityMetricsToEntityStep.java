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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.TemplateUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.StringTemplateConstants;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
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
import com.latticeengines.domain.exposed.spark.cdl.MergeActivityMetricsJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper;
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
    private DataCollection.Version active;
    private boolean shortCutMode = false;

    private ConcurrentMap<String, Map<String, DimensionMetadata>> streamMetadataCache;
    private final Map<TableRoleInCollection, Table> tableCache = new HashMap<>();
    private static final TypeReference<ConcurrentMap<String, Map<String, DimensionMetadata>>> streamMetadataCacheTypeRef = new TypeReference<ConcurrentMap<String, Map<String, DimensionMetadata>>>() {
    };
    private final Set<String> dateRangeEvaluatedSet = new HashSet<>();
    private final Set<Category> updatedCategories = new HashSet<>();
    private final Map<String, String> relinkedMetrics = new HashMap<>();

    @Override
    protected Class<? extends AbstractSparkJob<MergeActivityMetricsJobConfig>> getJobClz() {
        return MergeActivityMetrics.class;
    }

    @Override
    protected MergeActivityMetricsJobConfig configureJob(ActivityStreamSparkStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        active = inactive.complement();
        streamMetadataCache = JsonUtils.deserializeByTypeRef(getStringValueFromContext(ACTIVITY_STREAM_METADATA_CACHE), streamMetadataCacheTypeRef);
        Set<String> skippedStreams = getSkippedStreamIds();
        // TODO - link unchanged activity profiles
        List<ActivityMetricsGroup> groups = stepConfiguration.getActivityMetricsGroupMap().values().stream()
                .filter(g -> !skippedStreams.contains(g.getStream().getStreamId())).collect(Collectors.toList());

        Map<String, List<ActivityMetricsGroup>> mergedTablesMap = new HashMap<>(); // merged table label -> groups to merge
        Set<String> activityMetricsServingEntities = new HashSet<>();
        if (CollectionUtils.isEmpty(groups)) {
            log.info("No groups to merge for tenant {}. Skip merging metrics groups", customerSpace);
            return null;
        }
        groups.forEach(group -> {
            activityMetricsServingEntities.add(CategoryUtils.getEntity(group.getCategory()).get(0).name());
            String mergedTableLabel = getMergedLabel(group); // entity_servingStore e.g. Account_OpportunityProfile
            mergedTablesMap.putIfAbsent(mergedTableLabel, new ArrayList<>());
            mergedTablesMap.get(mergedTableLabel).add(group);
        });
        // for profiling merged tables
        putObjectInContext(ACTIVITY_MERGED_METRICS_SERVING_ENTITIES, activityMetricsServingEntities);
        Map<String, String> mergedMetricsGroupTableNames = getMapObjectFromContext(MERGED_METRICS_GROUP_TABLE_NAME, String.class, String.class);
        shortCutMode = allTablesExist(mergedMetricsGroupTableNames) && tableInHdfs(mergedMetricsGroupTableNames, false);
        if (shortCutMode) {
            Map<TableRoleInCollection, Map<String, String>> signatureTableNames = new HashMap<>();
            for (Map.Entry<String, String> entry : mergedMetricsGroupTableNames.entrySet()) {
                String mergedTableLabel = entry.getKey();
                String tableName = entry.getValue();
                TableRoleInCollection servingStore = getServingStoreInLable(mergedTableLabel);
                signatureTableNames.putIfAbsent(servingStore, new HashMap<>());
                signatureTableNames.get(servingStore).put(getEntityInLabel(mergedTableLabel).name(), tableName);
            }
            log.info("Retrieved merged metrics: {}. Going through shortcut mode.", mergedMetricsGroupTableNames);
            signatureTableNames.keySet().forEach(role -> dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames.get(role), role, inactive));
            return null;
        } else {
            SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
            Map<String, SparkIOMetadataWrapper.Partition> detailsMap = new HashMap<>();
            AtomicInteger index = new AtomicInteger();
            List<DataUnit> inputs = new ArrayList<>();
            mergedTablesMap.forEach((mergedTableLabel, groupsToMerge) -> {
                SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
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
            appendActiveActivityMetrics(inputs, inputMetadata, groups);
            config.setInput(inputs);
            return config;
        }
    }

    private void appendActiveActivityMetrics(List<DataUnit> inputs, SparkIOMetadataWrapper inputMetadata, List<ActivityMetricsGroup> groups) {
        Map<String, SparkIOMetadataWrapper.Partition> metadataMap = inputMetadata.getMetadata();
        groups.forEach(group -> {
            TableRoleInCollection servingStore = getServingStore(group.getCategory());
            if (!tableCache.containsKey(servingStore)) {
                tableCache.put(servingStore, dataCollectionProxy.getTable(customerSpace.toString(), servingStore, active));
            }
            Table activeMetrics = tableCache.get(servingStore);
            if (activeMetrics != null && metadataMap.get(servingStore.name()) == null) {
                log.info("Found activity metrics {} from previous version {}.", servingStore, active);
                SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
                details.setStartIdx(inputs.size());
                metadataMap.put(servingStore.name(), details);
                inputs.add(activeMetrics.toHdfsDataUnit(null));
            }
        });
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

    private String getMergedLabel(ActivityMetricsGroup group) { // entity_servingStore e.g. Account_OpportunityProfile
        BusinessEntity entity = group.getEntity();
        TableRoleInCollection servingStore = getServingStore(group.getCategory());
        return String.format("%s_%s", entity, servingStore);
    }

    private TableRoleInCollection getServingStore(Category category) {
        return CategoryUtils.getEntity(category).get(0).getServingStore();
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        if (shortCutMode) {
            return;
        }
        String outputMetadataStr = result.getOutput();
        log.info("Generated output metadata: {}", outputMetadataStr);
        log.info("Generated {} merged tables", result.getTargets().size());
        // entity_servingEntity -> table index, attributes to deprecate
        Map<String, SparkIOMetadataWrapper.Partition> outputMetadata = JsonUtils.deserialize(outputMetadataStr, SparkIOMetadataWrapper.class).getMetadata();
        Map<TableRoleInCollection, Map<String, String>> signatureTableNames = new HashMap<>();
        Map<String, Table> mergedMetricsGroupTables = new HashMap<>();
        outputMetadata.forEach((mergedTableLabel, details) -> {
            HdfsDataUnit output = result.getTargets().get(details.getStartIdx());
            String tableName = customerSpace.getTenantId() + "_" + NamingUtils.timestamp(getServingStoreInLable(mergedTableLabel).name());
            Table mergedTable = toTable(tableName, output);
            BusinessEntity entity = getEntityInLabel(mergedTableLabel);
            if (output.getCount() <= 0) {
                // create dummy record with meaningless accountId, append to mergedDU
                log.warn("Empty metrics found: {}. Append dummy record.", tableName);
                appendDummyRecord(mergedTable, entity);
            } else {
                enrichActivityAttributes(mergedTable, entity, getServingStoreInLable(mergedTableLabel), new HashSet<>(details.getLabels())); // labels are attributes to be deprecated
            }
            metadataProxy.createTable(customerSpace.toString(), tableName, mergedTable);
            TableRoleInCollection servingStore = getServingStoreInLable(mergedTableLabel);
            signatureTableNames.putIfAbsent(servingStore, new HashMap<>());
            signatureTableNames.get(servingStore).put(getEntityInLabel(mergedTableLabel).name(), tableName);
            mergedMetricsGroupTables.put(mergedTableLabel, mergedTable);
            log.info("Processed date ranges for {}: {}", servingStore, dateRangeEvaluatedSet);
        });
        updateDataRefreshTimeForCategories();
        mergedMetricsGroupTables.putAll(relinkedMetrics.entrySet().stream().map(entry -> {
            String groupId = entry.getKey();
            String tableName = entry.getValue();
            return Pair.of(groupId, getTableSummary(customerSpace.toString(), tableName));
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        // signature: entity (Account/Contact)
        // role: WebVisitProfile
        exportToS3AndAddToContext(mergedMetricsGroupTables, MERGED_METRICS_GROUP_TABLE_NAME);
        signatureTableNames.keySet().forEach(role -> dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames.get(role), role, inactive));
    }

    private void updateDataRefreshTimeForCategories() {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        Map<String, Long> dateMap = status.getDateMap();
        if (MapUtils.isEmpty(dateMap)) {
            log.error("No date map found in data collection status");
        } else if (!updatedCategories.isEmpty()) {
            long updateTime = getLongValueFromContext(PA_TIMESTAMP);
            updatedCategories.stream() //
                    .filter(Objects::nonNull) //
                    .forEach(category -> dateMap.put(category.getName(), updateTime));
            putObjectInContext(CDL_COLLECTION_STATUS, status);
        } else {
            log.warn("No category is updated, not setting refresh time");
        }
    }

    private void enrichActivityAttributes(Table mergedTable, BusinessEntity entity, TableRoleInCollection servingStore, Set<String> deprecatedAttrNames) {
        String entityIdAttrName = getEntityIdColName(entity);
        Map<String, Attribute> attributeMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(deprecatedAttrNames)) {
            Table activeMetricsTable = tableCache.get(servingStore);
            if (activeMetricsTable == null) {
                throw new RuntimeException(String.format("Failed to retrieve active metrics from type with serving store %s of version %s", servingStore, active));
            }
            log.warn("Mark attributes as deprecated as they are only found in previous version of activity metrics {}: {}", mergedTable.getName(), deprecatedAttrNames);
            activeMetricsTable.getAttributes().stream()
                    .filter(attr -> deprecatedAttrNames.contains(attr.getName()))
                    .forEach(attr -> attributeMap.put(attr.getName(), attr));
        }
        mergedTable.getAttributes().stream()
                .filter(attribute -> !entityIdAttrName.equalsIgnoreCase(attribute.getName()))
                .forEach(attr -> {
                    String attrName = attr.getName();
                    if (attributeMap.containsKey(attrName)) {
                        copyActivityAttributeMetadata(attributeMap.get(attrName), attr);
                        attr.setShouldDeprecate(true);
                        return;
                    }
                    List<String> tokens;
                    try {
                        tokens = ActivityMetricsGroupUtils.parseAttrName(attrName);
                    } catch (ParseException e) {
                        throw new IllegalArgumentException("Cannot parse metric attribute " + attrName, e);
                    }
                    String groupId = tokens.get(0);
                    String[] rollupDimVals = tokens.get(1).split("_");
                    String timeRange = tokens.get(2);

                    String evaluationDate = getStringValueFromContext(CDL_EVALUATION_DATE);
                    BusinessCalendar calendar = periodProxy.getBusinessCalendar(customerSpace.toString());
                    TimeFilterTranslator translator = new TimeFilterTranslator(getPeriodStrategies(calendar), evaluationDate);
                    enrichAttribute(attr, groupId, rollupDimVals, timeRange, translator);
                });
    }

    private void copyActivityAttributeMetadata(Attribute from, Attribute to) {
        to.setDisplayName(from.getDisplayName());
        to.setSecondaryDisplayName(from.getSecondaryDisplayName());
        to.setDescription(from.getDescription());
        to.setCategory(from.getCategory());
        to.setSubcategory(from.getSubcategory());
        to.setSecondarySubCategoryDisplayName(from.getSecondarySubCategoryDisplayName());
    }

    private void enrichAttribute(Attribute attr, String groupId, String[] rollupDimVals, String timeRange, TimeFilterTranslator translator) {
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
        Map<String, Object> params = createParamMap(rollupDimNames, rollupDimVals, timeRange, streamDimMetadata, attrName);
        String displayNameTmpl = group.getDisplayNameTmpl().getTemplate();
        if (StringUtils.isNotBlank(displayNameTmpl)) {
            try {
                attr.setDisplayName(TemplateUtils.renderByMap(displayNameTmpl, params));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to render display name for attribute " + attrName, e);
            }
        }
        String descTmpl = group.getDescriptionTmpl() == null ? null : group.getDescriptionTmpl().getTemplate();
        if (StringUtils.isNotBlank(descTmpl)) {
            try {
                attr.setDescription(TemplateUtils.renderByMap(descTmpl, params));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to render description for attribute " + attrName, e);
            }
        }
        String subCategoryTmpl = group.getSubCategoryTmpl().getTemplate();
        if (StringUtils.isNotBlank(subCategoryTmpl)) {
            try {
                attr.setSubcategory(TemplateUtils.renderByMap(subCategoryTmpl, params));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to render sub-category for attribute " + attrName, e);
            }
        }
        if (Category.WEB_VISIT_PROFILE.equals(group.getCategory()) && !group.getRollupDimensions().contains(InterfaceName.DerivedId.name())) { // TODO - this block is for backwards compatibility, should remove after merge and add secondary tmpl to existing web visit groups and keep only the one in else block
            String secondarySubCategoryTmpl = "${PathPatternId.PathPattern}";
            if (StringUtils.isNotBlank(secondarySubCategoryTmpl)) {
                try {
                    attr.setSecondarySubCategoryDisplayName(TemplateUtils.renderByMap(secondarySubCategoryTmpl, params));
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to render secondary sub-category display name for attribute " + attrName, e);
                }
            }
        } else {
            String secondarySubCategoryTmpl = group.getSecondarySubCategoryTmpl() == null ? null : group.getSecondarySubCategoryTmpl().getTemplate();
            if (StringUtils.isNotBlank(secondarySubCategoryTmpl)) {
                try {
                    attr.setSecondarySubCategoryDisplayName(TemplateUtils.renderByMap(secondarySubCategoryTmpl, params));
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to render secondary sub-category display name for attribute " + attrName, e);
                }
            }
        }
        setFundamentalType(attr, group);
        setAttrEvaluatedDate(attr, timeRange, translator);
        if (group.getCategory() != null) {
            updatedCategories.add(group.getCategory());
        }
    }

    private Map<String, Object> createParamMap(String[] rollupDimNames, String[] rollupDimVals, String timeRange, Map<String, DimensionMetadata> streamDimMetadata, String attrName) {
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
        try {
            String timeDesc = ActivityMetricsGroupUtils.timeRangeTmplToDescription(timeRange);
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_TIME_RANGE_TOKEN, timeDesc);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse time range for attribute " + attrName, e);
        }

        try {
            String nextTimeRangePeriodOnly = ActivityMetricsGroupUtils.timeRangeTmplToPeriodOnly(timeRange, 1);
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_NEXT_RANGE_PERIOD_ONLY_TOKEN, nextTimeRangePeriodOnly);
        } catch (Exception e) {
            // Do nothing for now
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_NEXT_RANGE_PERIOD_ONLY_TOKEN, Strings.EMPTY);
        }

        try {
            String timeRangePeriodOnly = ActivityMetricsGroupUtils.timeRangeTmplToPeriodOnly(timeRange, 0);
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_TIME_RANGE_PERIOD_ONLY_TOKEN, timeRangePeriodOnly);
        } catch (Exception e) {
            // Do nothing for now
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_TIME_RANGE_PERIOD_ONLY_TOKEN, Strings.EMPTY);
        }

        try {
            String periodStrategy = ActivityMetricsGroupUtils.getPeriodStrategyFromTimeRange(timeRange);
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_PERIOD_STRATEGY_TOKEN, periodStrategy);
        } catch (Exception e) {
            // Do nothing for now
            params.put(StringTemplateConstants.ACTIVITY_METRICS_GROUP_PERIOD_STRATEGY_TOKEN, Strings.EMPTY);
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

    private void setFundamentalType(@NotNull Attribute attr, ActivityMetricsGroup group) {
        if (attr.getFundamentalType() != null || group.getAggregation() == null) {
            // already have type set
            return;
        }
        attr.setFundamentalType(group.getAggregation().getTargetFundamentalType());
    }

    private void setAttrEvaluatedDate(Attribute attr, String timeRange, TimeFilterTranslator translator) {
        TimeFilter timeFilter = ActivityMetricsGroupUtils.timeRangeTmplToTimeFilter(timeRange);
        if (ComparisonType.EVER.equals(timeFilter.getRelation())) {
            dateRangeEvaluatedSet.add(ComparisonType.EVER.name());
            return;
        }
        Pair<Integer, Integer> periodIdRange = translator.translateRange(timeFilter);
        Pair<String, String> dateRange = translator.periodIdRangeToDateRange(timeFilter.getPeriod(), periodIdRange);
        String evaluatedDaterange = String.format(StringTemplateConstants.ACTIVITY_METRICS_ATTR_SECONDARY_DISPLAYNAME, dateRange.getLeft(), dateRange.getRight());
        attr.setSecondaryDisplayName(evaluatedDaterange);
        dateRangeEvaluatedSet.add(evaluatedDaterange);
    }

    private void appendDummyRecord(Table targetTable, BusinessEntity entity) {
        List<GenericRecord> dummyRecords = createDummyRecord(entity);
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

    private List<GenericRecord> createDummyRecord(BusinessEntity entity) {
        String entityId = BusinessEntity.Account.equals(entity) ? InterfaceName.AccountId.name() : InterfaceName.ContactId.name();
        String dummySchemaStr = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"" + entityId + "\",\"type\":[\"string\",\"null\"]}]}";
        Schema.Parser parser = new Schema.Parser();
        GenericRecord record = new GenericData.Record(parser.parse(dummySchemaStr));
        record.put(entityId, DataCloudConstants.ENTITY_ANONYMOUS_ID);
        return Collections.singletonList(record);
    }

    private TableRoleInCollection getServingStoreInLable(String mergedTableLabels) {
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

    private Set<String> getRelinkStreamIds() {
        if (!hasKeyInContext(ACTIVITY_STREAMS_RELINK)) {
            return Collections.emptySet();
        }
        Set<String> streams = getSetObjectFromContext(ACTIVITY_STREAMS_RELINK, String.class);
        log.info("Stream IDs to relink = {}", streams);
        return streams;
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
