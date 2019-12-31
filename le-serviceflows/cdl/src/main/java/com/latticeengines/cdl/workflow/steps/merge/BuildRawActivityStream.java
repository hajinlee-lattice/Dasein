package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(BuildRawActivityStream.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildRawActivityStream extends BaseActivityStreamStep {

    private static final Logger log = LoggerFactory.getLogger(BuildRawActivityStream.class);

    static final String BEAN_NAME = "buildRawActivityStream";

    private static final String RAWSTREAM_TABLE_PREFIX_FORMAT = "RawStream_%s";
    private static final String REMATCH_RAWSTREAM_TABLE_PREFIX_FORMAT = "Re_RawStream_%s";

    // streamId -> matched raw stream import table (in rematch, this means new
    // import + batch store)
    private Map<String, String> matchedStreamImportTables;
    // streamId -> table prefix of raw streams processed by transformation request
    private final Map<String, String> rawStreamTablePrefixes = new HashMap<>();
    private long paTimestamp;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        paTimestamp = getLongValueFromContext(PA_TIMESTAMP);
        log.info("Timestamp used as current time to build raw stream = {}", paTimestamp);
        log.info("IsRematch={}, isReplace={}", configuration.isRematchMode(), configuration.isReplaceMode());

        matchedStreamImportTables = getMatchedStreamImportTables();
        log.info("Matched raw stream import tables = {}", matchedStreamImportTables);
    }

    @Override
    protected void onPostTransformationCompleted() {
        if (isShortCutMode()) {
            return;
        }
        // TODO add diff report
        Map<String, String> rawStreamTables = buildRawStreamBatchStore();
        exportToS3AndAddToContext(rawStreamTables, RAW_ACTIVITY_STREAM_TABLE_NAME);
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

    private boolean isShortCutMode() {
        return hasKeyInContext(RAW_ACTIVITY_STREAM_TABLE_NAME);
    }

    private Map<String, String> getMatchedStreamImportTables() {
        if (!hasKeyInContext(ENTITY_MATCH_STREAM_TARGETTABLE)) {
            return Collections.emptyMap();
        }

        return getMapObjectFromContext(ENTITY_MATCH_STREAM_TARGETTABLE, String.class, String.class);
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
}
