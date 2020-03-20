package com.latticeengines.cdl.workflow.steps.merge;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component(PrepareForActivityStream.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class PrepareForActivityStream extends BaseWorkflowStep<ProcessActivityStreamStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PrepareForActivityStream.class);

    static final String BEAN_NAME = "prepareForActivityStream";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public void execute() {
        Map<String, AtlasStream> previousStreams = getPreviousStreams();
        Map<String, AtlasStream> currentStreams = getCurrentStreams();
        Set<String> streamsNeedRebuild = streamsNeedRebuild(previousStreams, currentStreams);
        putObjectInContext(ACTIVITY_STREAMS_NEED_REBUILD, streamsNeedRebuild);
        // TODO handle stream removal or rename (if we allows it) later
        // TODO need to handle preiod change here?
        log.info("Previous streams = {}, current streams = {}, streams need rebuild = {}",
                JsonUtils.serialize(previousStreams), JsonUtils.serialize(currentStreams), streamsNeedRebuild);
        saveStreamsToDataCollectionStatus(currentStreams);
    }

    /*-
     * return all stream names that requires rebuild
     */
    private Set<String> streamsNeedRebuild(@NotNull Map<String, AtlasStream> previousStreams,
            @NotNull Map<String, AtlasStream> currentStreams) {
        // treat every new stream as rebuild
        Set<String> streamNames = new HashSet<>(Sets.difference(currentStreams.keySet(), previousStreams.keySet()));

        // check whether common stream requires rebuild or not
        Set<String> commonStreamsToRebuild = currentStreams.entrySet() //
                .stream() //
                .filter(entry -> previousStreams.containsKey(entry.getKey())) //
                .filter(entry -> requireRebuild(previousStreams.get(entry.getKey()), entry.getValue())) //
                .map(Map.Entry::getKey) //
                .collect(Collectors.toSet());
        streamNames.addAll(commonStreamsToRebuild);
        return streamNames;
    }

    /*-
     * pass in the previous & current state of a stream, decide whether it needs rebuild
     */
    @VisibleForTesting
    boolean requireRebuild(@NotNull AtlasStream previous, @NotNull AtlasStream current) {
        Preconditions.checkNotNull(previous);
        Preconditions.checkNotNull(current);
        Preconditions.checkArgument(previous.getStreamId().equals(current.getStreamId()),
                "Stream names should be the same");

        String streamId = previous.getStreamId();
        log.info("Checking whether stream (PID={}, streamId={}, prevStreamName={}, currStreamName={}) should rebuild",
                previous.getPid(), streamId, previous.getName(), current.getName());
        if (current.getDataFeedTaskIngestionBehavior() == DataFeedTask.IngestionBehavior.Replace) {
            log.info("Ingestion behavior for stream {} is replace, require rebuild", streamId);
            return true;
        }
        if (!distinctEquals(previous.getMatchEntities(), current.getMatchEntities())) {
            logStreamChanged("match entities", streamId, previous.getAggrEntities(), current.getAggrEntities());
            return true;
        }
        if (!distinctEquals(previous.getAggrEntities(), current.getAggrEntities())) {
            logStreamChanged("aggregate entities", streamId, previous.getAggrEntities(), current.getAggrEntities());
            return true;
        }
        if (!StringUtils.equals(previous.getDateAttribute(), current.getDateAttribute())) {
            logStreamChanged("date attribute", streamId, previous.getDateAttribute(), current.getDateAttribute());
            return true;
        }
        if (!distinctEquals(previous.getDimensions(), current.getDimensions())) {
            logStreamChanged("dimensions", streamId, JsonUtils.serialize(previous.getDimensions()),
                    JsonUtils.serialize(current.getDimensions()));
            return true;
        }
        if (!distinctEquals(previous.getAttributeDerivers(), current.getAttributeDerivers())) {
            logStreamChanged("attribute derivers", streamId, previous.getAttributeDerivers(),
                    current.getAttributeDerivers());
            return true;
        }
        // TODO maybe check catalog data change

        return false;
    }

    private void logStreamChanged(@NotNull String varName, @NotNull String streamId, Object previousVal,
            Object currentVal) {
        log.info("{} changed for stream {}, require rebuild. previous={}, current={}", varName, streamId, previousVal,
                currentVal);
    }

    private <T> boolean distinctEquals(List<T> list1, List<T> list2) {
        Set<T> set1 = new HashSet<>(ListUtils.emptyIfNull(list1));
        Set<T> set2 = new HashSet<>(ListUtils.emptyIfNull(list2));
        return set1.equals(set2);
    }

    /*
     * Serialize current streams and save to data collection status in inactive
     * version
     */
    private void saveStreamsToDataCollectionStatus(@NotNull Map<String, AtlasStream> currentStreams) {
        String customerSpace = configuration.getCustomerSpace().toString();
        DataCollection.Version inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, inactive);
        status.setActivityStreamMap(currentStreams);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
        log.info("Save current activity streams {} to version {}", JsonUtils.serialize(currentStreams), inactive);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(customerSpace, status, inactive);
    }

    /*
     * Get activity streams used in last job (active version) return map of
     * streamName -> stream object
     */
    private Map<String, AtlasStream> getPreviousStreams() {
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus == null || MapUtils.isEmpty(dcStatus.getActivityStreamMap())) {
            return Collections.emptyMap();
        }

        return dcStatus.getActivityStreamMap();
    }

    /*-
     * Get activity streams associated to current job.
     * return map of streamName -> stream object
     */
    private Map<String, AtlasStream> getCurrentStreams() {
        if (configuration == null || MapUtils.isEmpty(configuration.getActivityStreamMap())) {
            return Collections.emptyMap();
        }

        return configuration.getActivityStreamMap();
    }
}
