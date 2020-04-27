package com.latticeengines.objectapi.service.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.xerial.snappy.Snappy;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.datafabric.FabricEntityFactory;
import com.latticeengines.domain.exposed.datafabric.GenericTableActivity;
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
    private static final String RANGE_KEY = "SortKey";
    private static final String RECORD_KEY = "Record";
    private static String TABLE_NAME = "_REPO_GenericTable_RECORD_GenericTableActivity_";
    public final String ID = "Id";
    private final String BLOB = "Record";

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
        return new DataPage(dynamoItemService.query(tableName, spec).stream().map(this::extractRecords)
                .filter(Objects::nonNull).map(GenericTableActivity::getAttributes).collect(Collectors.toList()));
    }

    private Object buildPartitionKey(String timelineId, String timeLineVersion, String entityId) {
        return TimeLineStoreUtils.generatePartitionKey(timeLineVersion, timelineId, entityId);
    }

    private GenericTableActivity extractRecords(Item item) {

        GenericRecord record = bytesToAvro(item.getByteBuffer(BLOB));
        Map<String, Object> tags = new HashMap<>();
        for (Map.Entry<String, Object> attr : item.attributes()) {
            if (attr.getValue() != null && !BLOB.equals(attr.getKey()) && !ID.equals(attr.getKey())) {
                tags.put(attr.getKey(), attr.getValue());
            }
        }
        return record != null
                ? FabricEntityFactory.pairToEntity(Pair.of(record, tags), GenericTableActivity.class,
                        getActivitySchema())
                : null;
    }

    private GenericRecord bytesToAvro(ByteBuffer byteBuffer) {
        try {
            ByteBuffer uncompressed = ByteBuffer.wrap(Snappy.uncompress(byteBuffer.array()));
            try (InputStream input = new ByteArrayInputStream(uncompressed.array())) {
                DatumReader<GenericRecord> reader = new GenericDatumReader<>(getActivitySchema());
                DataInputStream din = new DataInputStream(input);
                Decoder decoder = DecoderFactory.get().binaryDecoder(din, null);
                return reader.read(null, decoder);
            }
        } catch (Exception e) {
            log.warn("Exception in decoding generic record.", e);
            return null;
        }
    }

    private Schema getActivitySchema() {
        String recordType = GenericTableActivity.class.getSimpleName() + "_" + signature;
        return FabricEntityFactory.getFabricSchema(GenericTableActivity.class, recordType);
    }

    @VisibleForTesting
    void setTimeLineProxy(TimeLineProxy timeLineProxy) {
        this.timeLineProxy = timeLineProxy;
    }

    @VisibleForTesting
    void setDataCollectionProxy(DataCollectionProxy dataCollectionProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
    }

    @VisibleForTesting
    void setTableName(String tableName) {
        TABLE_NAME = tableName;
    }
}
