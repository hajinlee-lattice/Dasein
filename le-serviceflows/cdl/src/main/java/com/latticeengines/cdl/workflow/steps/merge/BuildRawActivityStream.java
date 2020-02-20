package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;

@Component(BuildRawActivityStream.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildRawActivityStream extends BaseActivityStreamStep<ProcessActivityStreamStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BuildRawActivityStream.class);

    static final String BEAN_NAME = "buildRawActivityStream";

    private static final String RAWSTREAM_TABLE_PREFIX_FORMAT = "RawStream_%s";
    private static final String REMATCH_RAWSTREAM_TABLE_PREFIX_FORMAT = "Re_RawStream_%s";

    // streamId -> matched raw stream import table (in rematch, this means new
    // import + batch store)
    private Map<String, String> matchedStreamImportTables;

    private long paTimestamp;

    private Map<String, String> updatedRawStreamTables;

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
