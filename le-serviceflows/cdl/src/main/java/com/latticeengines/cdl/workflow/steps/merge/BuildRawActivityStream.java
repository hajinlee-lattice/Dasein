package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_APPEND_RAWSTREAM;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.AppendRawStreamConfig;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(BuildRawActivityStream.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildRawActivityStream extends BaseMergeImports<ProcessActivityStreamStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BuildRawActivityStream.class);

    static final String BEAN_NAME = "buildRawActivityStream";

    private static final String RAWSTREAM_TABLE_PREFIX_FORMAT = "RawStream_%s";
    private static final String REMATCH_RAWSTREAM_TABLE_PREFIX_FORMAT = "Re_RawStream_%s";
    private static final List<String> RAWSTREAM_PARTITION_KEYS = ImmutableList.of(InterfaceName.__StreamDateId.name());

    // streamId -> set of column names
    private final Map<String, Set<String>> streamImportColumnNames = new HashMap<>();
    // streamId -> table prefix of raw streams processed by transformation request
    private final Map<String, String> rawStreamTablePrefixes = new HashMap<>();
    private long paTimestamp;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        paTimestamp = getLongValueFromContext(PA_TIMESTAMP);
        log.info("Timestamp used as current time to build raw stream = {}", paTimestamp);
        log.info("IsRematch={}, isReplace={}", configuration.isRematchMode(), configuration.isReplaceMode());
        buildStreamImportColumnNames();
        bumpEntityMatchStagingVersion();
    }

    @Override
    protected void onPostTransformationCompleted() {
        // TODO add diff report
        Map<String, String> rawStreamTables = buildRawStreamBatchStore();
        exportToS3AndAddToContext(rawStreamTables, RAW_ACTIVITY_STREAM_TABLE_NAME);
    }

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(BEAN_NAME);

        List<TransformationStepConfig> steps = new ArrayList<>();

        Map<String, Integer> matchedImportTableIdx = configuration.getStreamImports().entrySet() //
                .stream() //
                .map(entry -> {
                    String streamId = entry.getKey();
                    List<String> importTables = entry.getValue() //
                            .stream() //
                            .map(ActivityImport::getTableName) //
                            .collect(Collectors.toList());
                    if (CollectionUtils.isEmpty(importTables)) {
                        return null;
                    }

                    // add concat/match step
                    return Pair.of(streamId, concatAndMatchStreamImports(streamId, importTables, steps));
                }) //
                .filter(Objects::nonNull) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        // streams that have batch store and not ignored with replace
        Set<String> streamsWithActiveTableUsed = new HashSet<>();
        Map<String, Integer> appendedRawStreamStepIdx = new HashMap<>();
        // add date to import and append to active table
        configuration.getActivityStreamMap().forEach((streamId, stream) -> {
            DataFeedTask.IngestionBehavior behavior = stream.getDataFeedTaskIngestionBehavior();
            Integer matchedStepIdx = matchedImportTableIdx.get(streamId);
            String activeTable = configuration.getActiveRawStreamTables().get(streamId);
            // ignore active table in replace mode or stream has replace ingestion behavior
            if (configuration.isReplaceMode()) {
                activeTable = null;
            } else if (behavior == DataFeedTask.IngestionBehavior.Replace) {
                log.info("Stream {} has ingestion behavior replace, ignore active table {}", streamId, activeTable);
                activeTable = null;
            } else if (behavior != DataFeedTask.IngestionBehavior.Append) {
                String msg = String.format("Ingestion behavior %s for stream %s is not supported", behavior, streamId);
                throw new UnsupportedOperationException(msg);
            }
            if (activeTable != null) {
                streamsWithActiveTableUsed.add(streamId);
            }
            // append import to batch store
            appendRawStream(steps, stream, matchedStepIdx, activeTable, RAWSTREAM_TABLE_PREFIX_FORMAT)
                    .ifPresent(idx -> appendedRawStreamStepIdx.put(streamId, idx));
        });

        if (configuration.isRematchMode()) {
            log.info("Adding rematch steps, appendedRawStreamIdxMap = {}", appendedRawStreamStepIdx);

            configuration.getActivityStreamMap().forEach((streamId, stream) -> {
                if (!appendedRawStreamStepIdx.containsKey(streamId)) {
                    // no import & batch store
                    return;
                }

                String activeTable = configuration.getActiveRawStreamTables().get(streamId);
                int appendedStreamIdx = appendedRawStreamStepIdx.get(streamId);
                Set<String> importTableColumns = new HashSet<>(
                        streamImportColumnNames.getOrDefault(streamId, new HashSet<>()));
                if (streamsWithActiveTableUsed.contains(streamId)) {
                    importTableColumns.addAll(getTableColumnNames(activeTable));
                }
                // match with empty universe and use as import to append
                addMatchStep(stream, importTableColumns, steps, appendedStreamIdx, true);
                appendRawStream(steps, stream, steps.size() - 1, null, REMATCH_RAWSTREAM_TABLE_PREFIX_FORMAT);
            });
        }

        if (CollectionUtils.isEmpty(steps)) {
            log.info("No existing/new activity stream found, skip build raw stream step");
            return null;
        }

        request.setSteps(steps);
        return request;
    }

    private Optional<Integer> appendRawStream(@NotNull List<TransformationStepConfig> steps,
            @NotNull AtlasStream stream, Integer matchedImportIdx, String activeBatchTable, String prefixFormat) {
        if (matchedImportIdx == null && StringUtils.isBlank(activeBatchTable)) {
            log.info("No import and no active batch store for stream {}. Skip append raw stream step",
                    stream.getStreamId());
            return Optional.empty();
        }

        String streamId = stream.getStreamId();
        String rawStreamTablePrefix = String.format(prefixFormat, streamId);
        AppendRawStreamConfig config = new AppendRawStreamConfig();
        config.dateAttr = stream.getDateAttribute();
        config.retentionDays = stream.getRetentionDays();
        config.currentEpochMilli = paTimestamp;

        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_APPEND_RAWSTREAM);
        if (matchedImportIdx == null) {
            // no import, must have active table
            config.masterInputIdx = 0;
            addBaseTables(step, ImmutableList.of(RAWSTREAM_PARTITION_KEYS), activeBatchTable);
        } else {
            // has import, no necessarily has active table (first time import or replace
            // mode)
            config.matchedRawStreamInputIdx = 0;
            step.setInputSteps(Collections.singletonList(matchedImportIdx));
            if (activeBatchTable != null) {
                config.masterInputIdx = 1;
                addBaseTables(step, ImmutableList.of(RAWSTREAM_PARTITION_KEYS), activeBatchTable);
            }
        }

        // configure dest table
        setTargetTable(step, rawStreamTablePrefix);
        step.setTargetPartitionKeys(RAWSTREAM_PARTITION_KEYS);

        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        steps.add(step);

        rawStreamTablePrefixes.put(streamId, rawStreamTablePrefix);
        return Optional.of(steps.size() - 1);
    }

    private Integer concatAndMatchStreamImports(@NotNull String streamId, @NotNull List<String> importTables,
            @NotNull List<TransformationStepConfig> steps) {
        AtlasStream stream = configuration.getActivityStreamMap().get(streamId);
        Set<String> importTableColumns = streamImportColumnNames.get(streamId);
        Preconditions.checkNotNull(streamId, String.format("Stream %s is not in config", streamId));
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(stream.getMatchEntities()),
                String.format("Stream %s does not have match entities", streamId));

        // concat import
        steps.add(dedupAndConcatTables(null, false, importTables));
        addMatchStep(stream, importTableColumns, steps, steps.size() - 1, false);
        return steps.size() - 1;
    }

    private void addMatchStep(@NotNull AtlasStream stream, @NotNull Set<String> importTableColumns,
            @NotNull List<TransformationStepConfig> steps, int matchInputTableIdx, boolean isRematchMode) {
        String matchConfig;
        // match entity TODO maybe support rematch and entity match GA?
        if (stream.getMatchEntities().contains(Contact.name())) {
            // contact match
            matchConfig = MatchUtils.getAllocateIdMatchConfigForContact(customerSpace.toString(), getBaseMatchInput(),
                    importTableColumns, getSystemIds(BusinessEntity.Account), getSystemIds(BusinessEntity.Contact),
                    null, isRematchMode);
        } else if (stream.getMatchEntities().contains(Account.name())) {
            // account match
            matchConfig = MatchUtils.getAllocateIdMatchConfigForAccount(customerSpace.toString(), getBaseMatchInput(),
                    importTableColumns, getSystemIds(BusinessEntity.Account), null, isRematchMode);
        } else {
            log.error("Match entities {} in stream {} is not supported", stream.getMatchEntities(),
                    stream.getStreamId());
            throw new UnsupportedOperationException(
                    String.format("Match entities in stream %s are not supported", stream.getStreamId()));
        }
        steps.add(match(matchInputTableIdx, null, matchConfig));
    }

    private Map<String, String> buildRawStreamBatchStore() {
        // tables in current active version will be processed since we need to drop old
        // data even if no import, so all tables will be included in prefix
        if (MapUtils.isEmpty(rawStreamTablePrefixes)) {
            // no import and no existing batch store
            return Collections.emptyMap();
        }
        Map<String, String> rawStreamTableNames = rawStreamTablePrefixes.entrySet() //
                .stream() //
                .map(entry -> Pair.of(entry.getKey(), TableUtils.getFullTableName(entry.getValue(), pipelineVersion))) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        log.info("Building raw stream tables, tables={}, pipelineVersion={}", rawStreamTableNames, pipelineVersion);

        // link all tables and use streamId as signature
        dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), rawStreamTableNames, batchStore,
                inactive);
        return rawStreamTableNames;
    }

    private void buildStreamImportColumnNames() {
        Map<String, List<ActivityImport>> streamImports = configuration.getStreamImports();
        if (MapUtils.isEmpty(streamImports)) {
            return;
        }

        streamImports.forEach((streamId, imports) -> {
            String[] importTables = imports.stream().map(ActivityImport::getTableName).toArray(String[]::new);
            Set<String> columns = getTableColumnNames(importTables);
            log.info("Stream {} has {} imports, {} total columns", streamId, imports.size(), columns.size());
            streamImportColumnNames.put(streamId, columns);
        });
    }
}
