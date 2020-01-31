package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_APPEND_RAWSTREAM;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.AppendRawStreamConfig;
import com.latticeengines.domain.exposed.util.TableUtils;

/**
 * Base class for {@link AtlasStream} processing
 */
public abstract class BaseActivityStreamStep<T extends ProcessActivityStreamStepConfiguration>
        extends BaseMergeImports<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseActivityStreamStep.class);
    private static final List<String> RAWSTREAM_PARTITION_KEYS = ImmutableList.of(InterfaceName.__StreamDateId.name());

    // streamId -> table prefix of raw streams processed by transformation request
    protected final Map<String, String> rawStreamTablePrefixes = new HashMap<>();

    /*
     * Retrieve effective activity stream batch store in active version, based on
     * stream setting and PA mode
     */
    protected String getRawStreamActiveTable(@NotNull String streamId, @NotNull AtlasStream stream) {
        DataFeedTask.IngestionBehavior behavior = stream.getDataFeedTaskIngestionBehavior();
        String activeTable = configuration.getActiveRawStreamTables().get(streamId);
        // ignore active table in replace mode or stream has replace ingestion behavior
        if (shouldReturnEmpty(streamId, behavior, activeTable)) {
            return null;
        }
        return activeTable;
    }

    protected boolean shouldReturnEmpty(@NotNull String streamId, DataFeedTask.IngestionBehavior behavior, String activeTable) {
        if (configuration.isReplaceMode()) {
            return true;
        } else if (behavior == DataFeedTask.IngestionBehavior.Replace) {
            log.info("Stream {} has ingestion behavior replace, ignore active table {}", streamId, activeTable);
            return true;
        } else if (behavior != DataFeedTask.IngestionBehavior.Append) {
            String msg = String.format("Ingestion behavior %s for stream %s is not supported", behavior, streamId);
            throw new UnsupportedOperationException(msg);
        }
        return false;
    }

    /**
     * Append matched raw stream import to batch store in active version
     *
     * @param matchedImportTable
     *            table name of matched stream import, can be {@code null} (no
     *            imports for this stream)
     * @param activeBatchTable
     *            raw stream batch store table name in active version, can be
     *            {@code null}
     * @return [ merged/appended raw stream table name, result transformation step
     *         idx ]
     */
    Optional<Pair<String, Integer>> appendRawStream(@NotNull List<TransformationStepConfig> steps,
            @NotNull AtlasStream stream, @NotNull Long paTimestamp, String matchedImportTable, String activeBatchTable,
            String prefixFormat) {
        if (!needAppendRawStream(matchedImportTable, activeBatchTable)) {
            log.info("No matched import table and no active batch store for stream {}. Skip append raw stream step",
                    stream.getStreamId());
            return Optional.empty();
        }

        return appendRawStream(steps, stream, paTimestamp,
                getConfigureAppendRawStreamInputFn(matchedImportTable, activeBatchTable), prefixFormat);
    }

    /**
     * Append matched raw stream import to batch store in active version
     *
     * @param matchedImportIdx
     *            index of transformation step where the output is matched raw
     *            stream imports, can be {@code null} (no imports for this stream)
     * @param activeBatchTable
     *            raw stream batch store table name in active version, can be
     *            {@code null}
     * @return [ merged/appended raw stream table name, result transformation step
     *         idx ]
     */
    Optional<Pair<String, Integer>> appendRawStream(@NotNull List<TransformationStepConfig> steps,
            @NotNull AtlasStream stream, @NotNull Long paTimestamp, Integer matchedImportIdx, String activeBatchTable,
            String prefixFormat) {
        if (!needAppendRawStream(matchedImportIdx, activeBatchTable)) {
            log.info("No matched import table and no active batch store for stream {}. Skip append raw stream step",
                    stream.getStreamId());
            return Optional.empty();
        }

        return appendRawStream(steps, stream, paTimestamp,
                getConfigureAppendRawStreamInputFn(matchedImportIdx, activeBatchTable), prefixFormat);
    }

    private Optional<Pair<String, Integer>> appendRawStream(@NotNull List<TransformationStepConfig> steps,
            @NotNull AtlasStream stream, @NotNull Long paTimestamp,
            BiFunction<TransformationStepConfig, AppendRawStreamConfig, Void> configureStepInputFn,
            String prefixFormat) {
        String streamId = stream.getStreamId();
        String rawStreamTablePrefix = String.format(prefixFormat, streamId);
        AppendRawStreamConfig config = new AppendRawStreamConfig();
        config.dateAttr = stream.getDateAttribute();
        config.retentionDays = stream.getRetentionDays();
        config.currentEpochMilli = paTimestamp;

        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_APPEND_RAWSTREAM);

        // set matched table & active batch store table for raw stream as input
        configureStepInputFn.apply(step, config);

        // configure dest table
        setTargetTable(step, rawStreamTablePrefix);
        step.setTargetPartitionKeys(RAWSTREAM_PARTITION_KEYS);

        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        steps.add(step);

        return Optional.of(Pair.of(rawStreamTablePrefix, steps.size() - 1));
    }

    /*-
     * helpers to configure AppendRawStreamJob input & trxfmr step in different ways
     */

    private BiFunction<TransformationStepConfig, AppendRawStreamConfig, Void> getConfigureAppendRawStreamInputFn(
            String matchedImportTable, String activeBatchTable) {
        return (step, config) -> {
            if (StringUtils.isBlank(matchedImportTable)) {
                // no import, must have active table
                config.masterInputIdx = 0;
                addBaseTables(step, ImmutableList.of(RAWSTREAM_PARTITION_KEYS), activeBatchTable);
            } else {
                // has import, no necessarily has active table (first time import or replace
                // mode)
                config.matchedRawStreamInputIdx = 0;
                addBaseTables(step, matchedImportTable);
                if (activeBatchTable != null) {
                    config.masterInputIdx = 1;
                    addBaseTables(step, ImmutableList.of(RAWSTREAM_PARTITION_KEYS), activeBatchTable);
                }
            }
            return null;
        };
    }

    private BiFunction<TransformationStepConfig, AppendRawStreamConfig, Void> getConfigureAppendRawStreamInputFn(
            Integer matchedImportIdx, String activeBatchTable) {
        return (step, config) -> {
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
            return null;
        };
    }

    protected Map<String, String> buildRawStreamBatchStore() {
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

    /*-
     * helpers to check whether AppendRawStream step is necessary
     */

    private boolean needAppendRawStream(Integer matchedImportIdx, String activeBatchTable) {
        return matchedImportIdx != null || StringUtils.isNotBlank(activeBatchTable);
    }

    private boolean needAppendRawStream(String matchedImportTable, String activeBatchTable) {
        return StringUtils.isNotBlank(matchedImportTable) || StringUtils.isNotBlank(activeBatchTable);
    }
}
