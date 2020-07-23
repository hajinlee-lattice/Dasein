package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedActivityStream;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.activity.ActivityBookkeeping;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@Component(BuildRawActivityStream.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildRawActivityStream extends BaseActivityStreamStep<ProcessActivityStreamStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BuildRawActivityStream.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Value("${cdl.activity.quota}")
    private long defaultQuota;

    static final String BEAN_NAME = "buildRawActivityStream";

    private static final String RAWSTREAM_TABLE_PREFIX_FORMAT = "RawStream_%s";
    private static final String REMATCH_RAWSTREAM_TABLE_PREFIX_FORMAT = "Re_RawStream_%s";
    private static final String CDL_EVAL_DATE_FORMAT = "yyyy-MM-dd";

    // streamId -> matched raw stream import table (in rematch, this means new
    // import + batch store)
    private Map<String, String> matchedStreamImportTables;

    private Long evalTimeEpoch;

    private Map<String, String> updatedRawStreamTables;

    private String evaluationDate;
    private Integer evaluationDateId;
    private Map<String, Long> tenantOverride; // streamId -> count

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        String evalDate = getStringValueFromContext(CDL_EVALUATION_DATE);
        try {
            evalTimeEpoch = new SimpleDateFormat("yyyy-MM-dd").parse(evalDate).getTime();
        } catch (ParseException e) {
            throw new IllegalStateException(String.format("Unable to parse eval date %s", evalDate));
        }
        evaluationDate = getStringValueFromContext(CDL_EVALUATION_DATE);
        evaluationDateId = DateTimeUtils.dateToDayPeriod(evaluationDate);
        log.info("Timestamp used as current time to build raw stream = {}", evalTimeEpoch);
        log.info("IsRematch={}, isReplace={}", configuration.isRematchMode(), configuration.isReplaceMode());

        matchedStreamImportTables = getMatchedStreamImportTables();
        log.info("Matched raw stream import tables = {}", matchedStreamImportTables);
        if (softDeleteEntities.containsKey(entity)) {
            log.info("Soft delete performed for Activity Stream");
            // only streams performed soft delete have updatedRawStreamTables entry
            // using this context to identify which streams performed delete
            updatedRawStreamTables = getMapObjectFromContext(RAW_STREAM_TABLE_AFTER_DELETE, String.class, String.class);
            log.info("Updated raw stream tables = {}", updatedRawStreamTables);
        }
        // in replace mode, delete the records in document db
        if (Boolean.TRUE.equals(configuration.isReplaceMode())) {
            cdlAttrConfigProxy.removeAttrConfigByTenantAndEntity(customerSpace.toString(),
                    configuration.getMainEntity());
        }
    }

    @Override
    protected void onPostTransformationCompleted() {
        if (isShortCutMode()) {
            return;
        }
        // TODO add diff report
        Map<String, String> rawStreamTables = buildRawStreamBatchStore();
        exportToS3AndAddToContext(rawStreamTables, RAW_ACTIVITY_STREAM_TABLE_NAME);
        exportToS3AndAddToContext(matchedStreamImportTables, RAW_ACTIVITY_STREAM_DELTA_TABLE_NAME);
    }

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        if (isShortCutMode()) {
            Map<String, String> rawStreamTableNames = getMapObjectFromContext(RAW_ACTIVITY_STREAM_TABLE_NAME,
                    String.class, String.class);
            log.info("Already processed this step, use existing checkpoint. Raw stream tables = {}",
                    rawStreamTableNames);
            dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), rawStreamTableNames, batchStore,
                    inactive);

            Set<String> streamsToRelink = getSetObjectFromContext(ACTIVITY_STREAMS_RELINK, String.class);
            DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
            Map<String, Integer> refreshDateMap = getRefreshDateMap(status);
            if (CollectionUtils.isNotEmpty(streamsToRelink)) {
                relinkStreams(new ArrayList<>(streamsToRelink));
                streamsToRelink.stream().filter(streamId -> !streamsToRelink.contains(streamId)).forEach(streamId -> {
                    refreshDateMap.put(streamId, DateTimeUtils.fromEpochMilliToDateId(evalTimeEpoch));
                });
            }
            status.setActivityStreamLastRefresh(refreshDateMap);
            putObjectInContext(CDL_COLLECTION_STATUS, status);
            return null;
        }
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(BEAN_NAME);

        verifyImportLimit();
        List<TransformationStepConfig> steps = getBuildRawStreamSteps();

        if (CollectionUtils.isEmpty(steps)) {
            log.info("No existing/new activity stream found, skip build raw stream step");
            return null;
        }

        request.setSteps(steps);
        return request;
    }

    private List<TransformationStepConfig> getBuildRawStreamSteps() {
        List<TransformationStepConfig> steps = new ArrayList<>();
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        Map<String, Integer> refreshDateMap = getRefreshDateMap(status);
        List<String> streamsNeedReLink = new ArrayList<>();

        configuration.getActivityStreamMap().forEach((streamId, stream) -> {
            String matchedImportTable = matchedStreamImportTables.get(streamId);
            // ignore active table in rematch
            String activeTable = configuration.isRematchMode() ? null : getRawStreamActiveTable(streamId, stream);
            String targetTablePrefixFormat = configuration.isRematchMode() ? REMATCH_RAWSTREAM_TABLE_PREFIX_FORMAT
                    : RAWSTREAM_TABLE_PREFIX_FORMAT;
            if (StringUtils.isNotBlank(matchedImportTable) || needRefresh(refreshDateMap, streamId)
                    || deletePerformed()) {
                // has import, over 30 days not refreshed, or performed soft delete
                appendRawStream(steps, stream, evalTimeEpoch, matchedImportTable, activeTable, targetTablePrefixFormat)
                        .ifPresent(pair -> {
                            rawStreamTablePrefixes.put(streamId, pair.getLeft());
                            refreshDateMap.put(stream.getStreamId(),
                                    DateTimeUtils.fromEpochMilliToDateId(evalTimeEpoch));
                        });
            } else if (StringUtils.isNotBlank(activeTable)) {
                streamsNeedReLink.add(streamId);
            }
        });
        status.setActivityStreamLastRefresh(refreshDateMap);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
        relinkStreams(streamsNeedReLink);
        putObjectInContext(ACTIVITY_STREAMS_RELINK, new HashSet<>(streamsNeedReLink));
        return steps;
    }

    private boolean needRefresh(Map<String, Integer> refreshDateMap, String streamId) {
        int threshold = DateTimeUtils.subtractDays(evaluationDateId, 30);
        boolean needRefresh = refreshDateMap.get(streamId) != null && refreshDateMap.get(streamId) < threshold;
        if (needRefresh) {
            log.info("Stream {} has no new import but hasn't been refreshed for over 30 days since {}.", streamId,
                    DateTimeUtils.dayPeriodToDate(threshold));
        }
        return needRefresh;
    }

    private Map<String, Integer> getRefreshDateMap(DataCollectionStatus status) {
        Map<String, Integer> map = status.getActivityStreamLastRefresh();
        return map == null ? new HashMap<>() : map;
    }

    private void verifyImportLimit() {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        tenantOverride = getTenantOverride();
        ActivityBookkeeping bookkeeping = getAndCleanupBookkeeping(status);
        matchedStreamImportTables.keySet().forEach(streamId -> {
            log.info("Verifying quota for stream {}", streamId);
            bookkeeping.streamRecord.putIfAbsent(streamId, new HashMap<>());
            verifyStreamImportLimit(streamId, bookkeeping.streamRecord.get(streamId));
        });
        log.info("Updating bookkeeping to {}", bookkeeping.streamRecord);
        status.setActivityBookkeeping(bookkeeping);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
    }

    private Map<String, Long> getTenantOverride() {
        Camille camille = CamilleEnvironment.getCamille();
        String podId = CamilleEnvironment.getPodId();
        TypeReference<Map<String, Long>> tenantOverrideTypeRef = new TypeReference<Map<String, Long>>() {
        };
        try {
            Path path = PathBuilder.buildTenantActivityUploadQuotaConfigPath(podId, customerSpace);
            if (camille.exists(path)) {
                String tenantOverrideStr = camille.get(path).getData();
                log.info("Found tenant override quota: {}", tenantOverrideStr);
                return JsonUtils.deserializeByTypeRef(tenantOverrideStr, tenantOverrideTypeRef);
            } else {
                log.info("Tenant quota override not found. Using default");
                return null;
            }
        } catch (Exception e) {
            log.warn("Tenant override found but unable to read: ", e);
            return null;
        }
    }

    private void verifyStreamImportLimit(String streamId, Map<Integer, Long> records) {
        Integer lastMonthLower = DateTimeUtils.subtractDays(evaluationDateId, 29);

        Long existingCount = getRecordsCount(records, lastMonthLower);
        Long importedCount = getImportCount();

        log.info("Verifying imported rows count for stream {} doesn't exceed limit...", streamId);
        log.info("Uploaded {} records in total ({} - {})", existingCount, DateTimeUtils.dayPeriodToDate(lastMonthLower),
                evaluationDate);
        log.info("Imported {} new records", importedCount);

        long totalLimit = defaultQuota;
        if (MapUtils.isNotEmpty(tenantOverride) && tenantOverride.containsKey(streamId)) {
            totalLimit = tenantOverride.get(streamId);
            log.info("Found tenant override quota for stream: {}", totalLimit);
        }
        log.info("Using quota for stream{}: {}", streamId, totalLimit);
        if (existingCount + importedCount > totalLimit) {
            throw new IllegalStateException(String.format("Exceeds total row count quota of %s", totalLimit));
        }
        updateBookkeeping(records, importedCount);
    }

    private void updateBookkeeping(Map<Integer, Long> records, Long importedCount) {
        records.put(evaluationDateId, records.getOrDefault(evaluationDateId, 0L) + importedCount);
    }

    private Long getImportCount() {
        List<Table> importTables = getTableSummaries(customerSpace.toString(),
                new ArrayList<>(matchedStreamImportTables.values()));
        return importTables.stream().mapToLong(table -> table.getExtracts().get(0).getProcessedRecords()).sum();
    }

    private Long getRecordsCount(Map<Integer, Long> records, int lower) {
        long count = 0;
        for (int i = lower; i < evaluationDateId; i++) {
            count += records.getOrDefault(i, 0L);
        }
        return count;
    }

    private ActivityBookkeeping getAndCleanupBookkeeping(DataCollectionStatus status) {
        if (status.getActivityBookkeeping() == null) {
            ActivityBookkeeping bookkeeping = new ActivityBookkeeping();
            bookkeeping.streamRecord = new HashMap<>();
            status.setActivityBookkeeping(bookkeeping);
        }
        ActivityBookkeeping bookkeeping = status.getActivityBookkeeping();
        Integer lowerBound = DateTimeUtils.subtractDays(evaluationDateId, 30);
        log.info("Evaluation date is {}, Removing records before {}", evaluationDate,
                DateTimeUtils.dayPeriodToDate(lowerBound));
        matchedStreamImportTables.keySet().forEach(streamId -> {
            bookkeeping.streamRecord.putIfAbsent(streamId, new HashMap<>());
            Map<Integer, Long> records = bookkeeping.streamRecord.get(streamId);
            records.keySet().stream().filter(dateId -> dateId < lowerBound).collect(Collectors.toSet())
                    .forEach(records::remove);
        });
        log.info("Cleaned up bookkeeping: {}", bookkeeping.streamRecord);
        return bookkeeping;
    }

    private boolean isShortCutMode() {
        return hasKeyInContext(RAW_ACTIVITY_STREAM_TABLE_NAME);
    }

    private Map<String, String> getMatchedStreamImportTables() {
        if (!hasKeyInContext(ENTITY_MATCH_STREAM_TARGETTABLE)) {
            return Collections.emptyMap();
        }

        return getMapObjectFromContext(ENTITY_MATCH_STREAM_TARGETTABLE, String.class, String.class);
    }

    @Override
    protected String getRawStreamActiveTable(@NotNull String streamId, @NotNull AtlasStream stream) {
        if (hardDeleteEntities.containsKey(entity)) {
            return null;
        }
        if (MapUtils.isEmpty(updatedRawStreamTables) || !updatedRawStreamTables.containsKey(streamId)) {
            return super.getRawStreamActiveTable(streamId, stream);
        } else {
            DataFeedTask.IngestionBehavior behavior = stream.getDataFeedTaskIngestionBehavior();
            String activeTable = updatedRawStreamTables.get(streamId);
            if (shouldReturnEmpty(streamId, behavior, activeTable)) {
                return null;
            }
            return activeTable;
        }
    }

    private boolean deletePerformed() {
        return softDeleteEntities.containsKey(entity) || hardDeleteEntities.containsKey(entity);
    }

    private void relinkStreams(List<String> streamsNeedReLink) {
        Map<String, String> signatureTableNames = dataCollectionProxy.getTableNamesWithSignatures(
                customerSpace.toString(), ConsolidatedActivityStream, active, streamsNeedReLink);
        if (MapUtils.isNotEmpty(signatureTableNames)) {
            log.info("Linking existing raw stream tables to inactive version {}: {}", inactive,
                    signatureTableNames.keySet());
            dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames,
                    ConsolidatedActivityStream, inactive);
        }
    }
}
