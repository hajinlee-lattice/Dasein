package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.DeriveActivityMetricGroupJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper;
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper.Partition;
import com.latticeengines.domain.exposed.util.CategoryUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.MetricsGroupGenerator;

@Component("metricsGroupsGenerationStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class MetricsGroupsGenerationStep extends RunSparkJob<ActivityStreamSparkStepConfiguration, DeriveActivityMetricGroupJobConfig> {

    private static final Logger log = LoggerFactory.getLogger(MetricsGroupsGenerationStep.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Inject
    private PeriodProxy periodProxy;

    private ConcurrentMap<String, Map<String, DimensionMetadata>> streamMetadataCache;

    private DataCollection.Version inactive;
    private String signature;
    private boolean shortCutMode = false;
    private final Map<String, String> relinkedGroupsTables = new HashMap<>();


    @Override
    protected Class<? extends AbstractSparkJob<DeriveActivityMetricGroupJobConfig>> getJobClz() {
        return MetricsGroupGenerator.class;
    }

    @Override
    protected DeriveActivityMetricGroupJobConfig configureJob(ActivityStreamSparkStepConfiguration stepConfiguration) {
        Set<String> skippedStreams = getSkippedStreamIds();
        Set<String> streamsToRelink = getRelinkStreamIds();
        List<AtlasStream> streams = stepConfiguration.getActivityStreamMap().values().stream()
                .filter(s -> !skippedStreams.contains(s.getStreamId()) && !streamsToRelink.contains(s.getStreamId())).collect(Collectors.toList());
        List<ActivityMetricsGroup> allGroups = stepConfiguration.getActivityMetricsGroupMap().values().stream()
                .filter(g -> !skippedStreams.contains(g.getStream().getStreamId())).collect(Collectors.toList());
        List<ActivityMetricsGroup> groupsNeedProcess = configuration.isShouldRebuild() ? allGroups :
                allGroups.stream().filter(g -> !streamsToRelink.contains(g.getStream().getStreamId())).collect(Collectors.toList());
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        relinkGroup(allGroups.stream().filter(g -> streamsToRelink.contains(g.getStream().getStreamId())).collect(Collectors.toList()));
        Set<BusinessEntity> requiredBatchStoreEntities = groupsNeedProcess.stream().map(ActivityMetricsGroup::getEntity).collect(Collectors.toSet());
        for (ActivityMetricsGroup group : groupsNeedProcess) {
            log.info("Retrieved group {}", group.getGroupId());
        }
        if (CollectionUtils.isEmpty(streams) || CollectionUtils.isEmpty(groupsNeedProcess)) {
            log.info("No groups to generate for tenant {}. Skip generating metrics groups", customerSpace);
            return null;
        }
        signature = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class).getDimensionMetadataSignature();
        streamMetadataCache = new ConcurrentHashMap<>();
        streams.forEach(this::updateStreamMetadataCache);
        putStringValueInContext(ACTIVITY_STREAM_METADATA_CACHE, JsonUtils.serialize(streamMetadataCache));
        SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
        Map<String, Partition> detailsMap = new HashMap<>();
        int idx = 0;
        for (AtlasStream stream : streams) {
            String streamId = stream.getStreamId();
            List<String> periods = stream.getPeriods();

            Partition details = new Partition();
            details.setStartIdx(idx);
            details.setLabels(periods);
            detailsMap.put(streamId, details);

            idx += periods.size();
        }
        inputMetadata.setMetadata(detailsMap);
        List<DataUnit> inputs = new ArrayList<>();
        Map<String, Table> periodStoreTableMap = getTablesFromMapCtxKey(customerSpace.toString(), PERIOD_STORE_TABLE_NAME);
        inputMetadata.getMetadata().forEach((streamId, details) ->
            details.getLabels().forEach(period -> {
                String ctxKey = String.format(PERIOD_STORE_TABLE_FORMAT, streamId, period);
                inputs.add(periodStoreTableMap.get(ctxKey).partitionedToHdfsDataUnit(null, Collections.singletonList(InterfaceName.PeriodId.name())));
            })
        );
        if (CollectionUtils.isEmpty(inputs)) {
            log.warn("No period store tables found. Skip metrics generation.");
            return null;
        }
        Map<String, String> groupTableNames = getMapObjectFromContext(METRICS_GROUP_TABLE_NAME, String.class, String.class);
        shortCutMode = allTablesExist(groupTableNames) && tableInHdfs(groupTableNames, false);
        if (shortCutMode) {
            log.info("Retrieved metrics {}. Going through shortcut mode.", groupTableNames);
            dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), groupTableNames, TableRoleInCollection.MetricsGroup, inactive);
            return null;
        } else {
            validateInputTableCountMatch(groupsNeedProcess, inputs, stepConfiguration);
            DeriveActivityMetricGroupJobConfig config = new DeriveActivityMetricGroupJobConfig();
            config.activityMetricsGroups = groupsNeedProcess;
            config.evaluationDate = getStringValueFromContext(CDL_EVALUATION_DATE);
            config.streamMetadataMap = streamMetadataCache;
            requiredBatchStoreEntities.forEach(entity -> appendBatchStore(entity, inputs, inputMetadata));
            config.setInput(inputs);
            config.inputMetadata = inputMetadata;
            config.businessCalendar = periodProxy.getBusinessCalendar(customerSpace.toString());
            config.currentVersionStamp = getLongValueFromContext(PA_TIMESTAMP);
            return config;
        }
    }

    private void relinkGroup(List<ActivityMetricsGroup> groups) {
        if (CollectionUtils.isNotEmpty(groups)) {
            List<String> signatures = groups.stream().map(ActivityMetricsGroup::getGroupId).collect(Collectors.toList());
            log.info("Groups to relink to inactive version: {}", signatures);
            Map<String, String> signatureTableNames = dataCollectionProxy.getTableNamesWithSignatures(customerSpace.toString(), TableRoleInCollection.MetricsGroup, inactive.complement(), signatures);
            if (MapUtils.isNotEmpty(signatureTableNames)) {
                log.info("Linking existing metrics group tables to inactive version: {}", signatureTableNames.keySet());
                dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames, TableRoleInCollection.MetricsGroup, inactive);
                relinkedGroupsTables.putAll(signatureTableNames);
            }
        }
    }

    private void appendBatchStore(BusinessEntity entity, List<DataUnit> inputs, SparkIOMetadataWrapper inputMetadata) {
        Table batchStoreTable = getBatchStoreTable(entity);
        if (batchStoreTable != null) {
            inputs.add(batchStoreTable.toHdfsDataUnit(BusinessEntity.Account.name()));
            Partition accountBatchStoreDetails = new Partition();
            accountBatchStoreDetails.setStartIdx(inputs.size() - 1);
            inputMetadata.getMetadata().put(entity.name(), accountBatchStoreDetails);
        } else {
            log.warn("{} batch store is missing. Groups for this entity will be skipped", entity);
        }
    }

    private Table getBatchStoreTable(BusinessEntity entity) {
        String batchStoreName;
        TableRoleInCollection batchStore = entity.getBatchStore();
        batchStoreName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, inactive);
        if (StringUtils.isBlank(batchStoreName)) {
            batchStoreName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, inactive.complement());
        }
        if (StringUtils.isBlank(batchStoreName)) {
            return null;
        }
        return metadataProxy.getTableSummary(customerSpace.toString(), batchStoreName);
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        if (shortCutMode) {
            return;
        }
        String outputMetadataStr = result.getOutput();
        log.info("Generated output metadata: {}", outputMetadataStr);
        log.info("Generated {} output metrics tables", result.getTargets().size());
        Map<String, Partition> outputMetadata = JsonUtils.deserialize(outputMetadataStr, SparkIOMetadataWrapper.class).getMetadata();
        Map<String, Table> signatureTables = new HashMap<>();
        Set<String> entityIds = ImmutableSet.of(InterfaceName.AccountId.name(), InterfaceName.ContactId.name());
        Map<BusinessEntity, List<String>> servingEntityCategoricalAttrs = new HashMap<>();
        Map<BusinessEntity, List<String>> servingEntityCategories = new HashMap<>();
        outputMetadata.forEach((groupId, details) -> {
            HdfsDataUnit metricsGroupDU = result.getTargets().get(details.getStartIdx());
            String ctxKey = String.format(METRICS_GROUP_TABLE_FORMAT, groupId);
            String tableName = TableUtils.getFullTableName(ctxKey, HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
            Table metricsGroupTable = toTable(tableName, metricsGroupDU);
            metadataProxy.createTable(customerSpace.toString(), tableName, metricsGroupTable);
            signatureTables.put(groupId, metricsGroupTable); // use groupId as signature
            ActivityMetricsGroup group = configuration.getActivityMetricsGroupMap().get(groupId);
            if (String.class.getSimpleName().equalsIgnoreCase(group.getJavaClass())) {
                BusinessEntity servingEntity = CategoryUtils.getEntity(group.getCategory()).get(0);
                servingEntityCategoricalAttrs.putIfAbsent(servingEntity,
                        metricsGroupTable.getAttributes().stream().map(Attribute::getName)
                                .filter(attrName -> !entityIds.contains(attrName)).collect(Collectors.toList()));
                servingEntityCategories.putIfAbsent(servingEntity,
                        new ArrayList<>(group.getCategorizeValConfig().getCategoryNames()));
            }
        });
        putObjectInContext(ACTIVITY_METRICS_CATEGORICAL_ATTR, servingEntityCategoricalAttrs);
        putObjectInContext(ACTIVITY_METRICS_CATEGORIES, servingEntityCategories);
        signatureTables.putAll(relinkedGroupsTables.entrySet().stream().map(entry -> {
            String groupId = entry.getKey();
            String tableName = entry.getValue();
            return Pair.of(groupId, getTableSummary(customerSpace.toString(), tableName));
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        Map<String, String> signatureTableNames = exportToS3AndAddToContext(signatureTables, METRICS_GROUP_TABLE_NAME);
        dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames, TableRoleInCollection.MetricsGroup, inactive);
    }

    private void validateInputTableCountMatch(List<ActivityMetricsGroup> groups, List<DataUnit> inputs, ActivityStreamSparkStepConfiguration stepConfiguration) {
        Set<String> streamIds = groups.stream().map(group -> group.getStream().getStreamId()).collect(Collectors.toSet());
        List<AtlasStream> streams = streamIds.stream().map(streamId -> stepConfiguration.getActivityStreamMap().get(streamId)).collect(Collectors.toList());
        int expectedPeriodStoresCount = streams.stream().mapToInt(stream -> stream.getPeriods().size()).sum();
        int actualPeriodStoresCount = inputs.size();
        log.info("Required total of {} period tables.", expectedPeriodStoresCount);
        log.info("Found total of {} period tables.", actualPeriodStoresCount);
        if (expectedPeriodStoresCount != actualPeriodStoresCount) {
            throw new IllegalStateException(String.format("Actual number of period tables not match. tenant=%s", customerSpace));
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

    private void updateStreamMetadataCache(AtlasStream stream) {
        if (!streamMetadataCache.containsKey(stream.getStreamId())) {
            streamMetadataCache.put(stream.getStreamId(), activityStoreProxy.getDimensionMetadataInStream(customerSpace.toString(), stream.getName(), signature));
        }
    }
}

