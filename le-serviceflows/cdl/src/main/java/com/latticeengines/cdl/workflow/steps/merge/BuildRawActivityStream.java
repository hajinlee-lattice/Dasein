package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
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
import com.latticeengines.domain.exposed.cdl.PeriodStrategy.Template;
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
    private String defaultQuotaStr;

    static final String BEAN_NAME = "buildRawActivityStream";

    private static final String RAWSTREAM_TABLE_PREFIX_FORMAT = "RawStream_%s";
    private static final String REMATCH_RAWSTREAM_TABLE_PREFIX_FORMAT = "Re_RawStream_%s";

    // streamId -> matched raw stream import table (in rematch, this means new
    // import + batch store)
    private Map<String, String> matchedStreamImportTables;

    private long paTimestamp;

    private Map<String, String> updatedRawStreamTables;

    private String evaluationDate;
    private Integer evaluationDateId;
    private Map<String, Long> defaultQuotaMap; // Month/Week/Day -> count
    private Map<String, Map<String, Long>> tenantOverride; // streamId -> quotaMap

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        paTimestamp = getLongValueFromContext(PA_TIMESTAMP);
        log.info("Timestamp used as current time to build raw stream = {}", paTimestamp);
        log.info("IsRematch={}, isReplace={}", configuration.isRematchMode(), configuration.isReplaceMode());

        matchedStreamImportTables = getMatchedStreamImportTables();
        log.info("Matched raw stream import tables = {}", matchedStreamImportTables);
        if (softDeleteEntities.containsKey(entity)) {
            log.info("Soft delete performed for Activity Stream");
            updatedRawStreamTables = getMapObjectFromContext(RAW_STREAM_TABLE_AFTER_DELETE, String.class, String.class);
            log.info("Updated raw stream tables = {}", updatedRawStreamTables);
        }
        // in replace mode, delete the records in document db
        if (Boolean.TRUE.equals(configuration.isReplaceMode())) {
            cdlAttrConfigProxy.removeAttrConfigByTenantAndEntity(customerSpace.toString(), configuration.getMainEntity());
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
            log.info("Already processed this step, use existing checkpoint. Raw stream tables = {}",
                    getMapObjectFromContext(RAW_ACTIVITY_STREAM_TABLE_NAME, String.class, String.class));
            return null;
        }
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(BEAN_NAME);

        List<TransformationStepConfig> steps = new ArrayList<>();

        verifyImportLimit();

        configuration.getActivityStreamMap().forEach((streamId, stream) -> {
            String matchedImportTable = matchedStreamImportTables.get(streamId);
            // ignore active table in rematch
            String activeTable = configuration.isRematchMode() ? null : getRawStreamActiveTable(streamId, stream);
            String targetTablePrefixFormat = configuration.isRematchMode() ? REMATCH_RAWSTREAM_TABLE_PREFIX_FORMAT
                    : RAWSTREAM_TABLE_PREFIX_FORMAT;
            appendRawStream(steps, stream, paTimestamp, matchedImportTable, activeTable, targetTablePrefixFormat) //
                    .ifPresent(pair -> rawStreamTablePrefixes.put(streamId, pair.getLeft()));
        });

        if (CollectionUtils.isEmpty(steps)) {
            log.info("No existing/new activity stream found, skip build raw stream step");
            return null;
        }

        request.setSteps(steps);
        return request;
    }

    private void verifyImportLimit() {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        defaultQuotaMap = getDefaultQuotaMap();
        tenantOverride = getTenantOverride();
        evaluationDate = getStringValueFromContext(CDL_EVALUATION_DATE);
        evaluationDateId = DateTimeUtils.dateToDayPeriod(evaluationDate);
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

    private Map<String, Long> getDefaultQuotaMap() {
        TypeReference<Map<String, Long>> quotaMapTypeRef = new TypeReference<Map<String, Long>>() {
        };
        log.info("Default quota: {}", defaultQuotaStr);
        return JsonUtils.deserializeByTypeRef(defaultQuotaStr, quotaMapTypeRef);
    }

    private Map<String, Map<String, Long>> getTenantOverride() {
        Camille camille = CamilleEnvironment.getCamille();
        String podId = CamilleEnvironment.getPodId();
        TypeReference<Map<String, Map<String, Long>>> tenantOverrideTypeRef = new TypeReference<Map<String, Map<String, Long>>>() {
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
            log.error("Tenant override found but unable to read: ", e);
            throw new RuntimeException("Failed to read tenant override");
        }
    }

    private void verifyStreamImportLimit(String streamId, Map<Integer, Long> records) {
        Integer lastMonthLower = DateTimeUtils.subtractDays(evaluationDateId, 29);
        Integer lastWeekLower = DateTimeUtils.subtractDays(evaluationDateId, 6);

        Long monthCount = getRecordsCount(records, lastMonthLower);
        Long weekCount = getRecordsCount(records, lastWeekLower);
        Long dayCount = getRecordsCount(records, evaluationDateId);
        Long importedCount = getImportCount();

        log.info("Verifying imported rows count for stream {} doesn't exceed limit...", streamId);
        log.info("Uploaded {} records in this month ({} - {})", monthCount, DateTimeUtils.dayPeriodToDate(lastMonthLower), evaluationDate);
        log.info("Uploaded {} records in this week ({} - {})", weekCount, DateTimeUtils.dayPeriodToDate(lastWeekLower), evaluationDate);
        log.info("Uploaded {} records today (not including new imports)", dayCount);
        log.info("Imported {} new records", importedCount);

        Map<String, Long> quota = defaultQuotaMap;
        if (tenantOverride != null && tenantOverride.containsKey(streamId)) {
            log.info("Found tenant override quota for stream.");
            quota.putAll(tenantOverride.get(streamId));
        }
        log.info("Using quota {}", quota);
        Long monthlyLimit = quota.get(Template.Month.name());
        Long weeklyLimit = quota.get(Template.Week.name());
        Long dailyLimit = quota.get(Template.Day.name());
        Long totalLimit = quota.get("Total");
        if (monthCount + importedCount > monthlyLimit) {
            throw new IllegalStateException(String.format("Exceeds monthly quota of %s", monthlyLimit));
        }
        if (weekCount + importedCount > weeklyLimit) {
            throw new IllegalStateException(String.format("Exceeds weekly quota of %s", weeklyLimit));
        }
        if (dayCount + importedCount > dailyLimit) {
            throw new IllegalStateException(String.format("Exceeds daily quota of %s", dailyLimit));
        }
        if (monthCount + importedCount >totalLimit) {
            throw new IllegalStateException(String.format("Exceeds total row count quota of %s", totalLimit));
        }
        updateBookkeeping(records, importedCount);
    }

    private void updateBookkeeping(Map<Integer, Long> records, Long importedCount) {
        records.put(evaluationDateId, records.getOrDefault(evaluationDateId, 0L) + importedCount);
    }

    private Long getImportCount() {
        List<Table> importTables = getTableSummaries(customerSpace.toString(), new ArrayList<>(matchedStreamImportTables.values()));
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
        log.info("Evaluation date is {}, Removing records before {}", evaluationDate, DateTimeUtils.dayPeriodToDate(lowerBound));
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
}
