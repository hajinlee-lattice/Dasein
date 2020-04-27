package com.latticeengines.objectapi.service.impl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.objectapi.service.ActivityTimelineQueryService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.TimeLineProxy;

@Component("timelineQueryService")
public class ActivityTimelineQueryServiceImpl implements ActivityTimelineQueryService {

    private static final Logger log = LoggerFactory.getLogger(ActivityTimelineQueryServiceImpl.class);

    @Inject
    private DynamoItemService dynamoItemService;

    @Inject
    private TimeLineProxy timeLineProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Value("${eai.export.dynamo.timeline.signature}")
    private String signature;

    private static final String PARTITION_KEY_TEMPLATE = "{0}_{1}_{2}_{3}";
    private static final String PARTITION_KEY = "PartitionKey";
    private static final String RANGE_KEY = "RangeKey";
    private static final String RECORD_KEY = "RecordKey";
    private static final String TABLE_NAME = "TimelineQueryServiceImplTestNG_dev_jlmehta";

    @SuppressWarnings("unchecked")
    public DataPage getData(String customerSpace, DataCollection.Version version,
            ActivityTimelineQuery activityTimelineQuery) {
        TimeLine timeline = timeLineProxy.findByEntity(customerSpace, activityTimelineQuery.getMainEntity());
        if (timeline == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format("No registered timeline found for entity %s for customerspace %s",
                            activityTimelineQuery.getMainEntity().name(), customerSpace) });
        }

        // TODO: switch with timeline version when available
        String timeLineVersion = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, version).getDetail()
                .getTimelineVersionMap().getOrDefault(timeline.getTimelineId(), null);

        if (StringUtils.isBlank(timeLineVersion)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format(
                            "No registered timeline version found for timelineId: %s, entity %s for customerspace %s",
                            timeline.getTimelineId(), activityTimelineQuery.getMainEntity().name(), customerSpace) });
        }

        if (StringUtils.isBlank(timeLineVersion)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format("No timeline version found for entity %s for customerspace %s",
                            activityTimelineQuery.getMainEntity().name(), customerSpace) });
        }

        QuerySpec spec = new QuerySpec() //
                .withHashKey(PARTITION_KEY,
                        buildPartitionKey(timeline.getTimelineId(), timeLineVersion,
                                activityTimelineQuery.getEntityId()))
                .withRangeKeyCondition(new RangeKeyCondition(RANGE_KEY).between(
                        activityTimelineQuery.getStartTimeStamp().toEpochMilli() + "",
                        activityTimelineQuery.getEndTimeStamp().toEpochMilli() + ""))
                .withScanIndexForward(false);
        String tableName = TABLE_NAME + signature;
        List<Item> items = dynamoItemService.query(tableName, spec);

        return new DataPage(
                items.stream().map(item -> (Map<String, Object>) item.get(RECORD_KEY)).collect(Collectors.toList()));
    }

    private Object buildPartitionKey(String timelineId, String timeLineVersion, String entityId) {
        return TimeLineStoreUtils.generatePartitionKey(timeLineVersion, timelineId, entityId);
    }

    @VisibleForTesting
    void setTimeLineProxy(TimeLineProxy timeLineProxy) {
        this.timeLineProxy = timeLineProxy;
    }

    @VisibleForTesting
    void setDataCollectionProxy(DataCollectionProxy dataCollectionProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
    }
}
