package com.latticeengines.objectapi.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.datafabric.GenericTimeseriesRecord;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusDetail;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.objectapi.service.ActivityTimelineQueryService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.TimeLineProxy;

public class ActivityTimelineQueryServiceImplTestNG extends QueryServiceImplTestNGBase {

    @Value("${common.le.environment}")
    private String env;

    @Value("${common.le.stack}")
    private String stack;

    private static final String TENANT_ID = "Test_Tenant";
    private static final String TIMELINE_ID = "Test_Timeline";
    private static final String VERSION_ID = "version";

    @Inject
    private DynamoService dynamoService;

    @Inject
    private DynamoItemService dynamoItemService;

    @Inject
    private ActivityTimelineQueryService activityTimelineQueryService;

    @Inject
    private TimeLineProxy timeLineProxy;

    @Value("${eai.export.dynamo.timeline.signature}")
    private String signature;

    private String tableName;

    @BeforeClass(groups = "functional", enabled = false)
    private void setup() {
        tableName = "TimelineQueryServiceImplTestNG_" + env + "_" + stack + signature;
        dynamoService.deleteTable(tableName);
        if (!dynamoService.hasTable(tableName)) {

            long readCapacityUnits = 10;
            long writeCapacityUnits = 10;
            String partitionKeyType = ScalarAttributeType.S.name();
            String sortKeyType = ScalarAttributeType.S.name();
            dynamoService.createTable(tableName, readCapacityUnits, writeCapacityUnits,
                    GenericTimeseriesRecord.PARTITION_KEY_ATTR, partitionKeyType,
                    GenericTimeseriesRecord.RANGE_KEY_ATTR, sortKeyType);

            List<Item> events = new ArrayList<>();
            long start = Instant.now().minus(90, ChronoUnit.DAYS).toEpochMilli();
            long end = Instant.now().toEpochMilli();
            for (int i = 0; i < 10000; i++) {
                GenericTimeseriesRecord event = new GenericTimeseriesRecord();
                event.setPartitionKey(TimeLineStoreUtils.generatePartitionKey(VERSION_ID, TIMELINE_ID,
                        Integer.valueOf(i % 2).toString()));
                event.setRangeKeyTimestamp(Instant.ofEpochMilli(ThreadLocalRandom.current().nextLong(start, end)));
                event.setRangeKeyID(UUID.randomUUID().toString());
                Map<String, Object> record = new HashMap<>();
                record.put("EventTimestamp", event.getRangeKeyTimestamp().toEpochMilli());
                record.put("Source", "Salesforce");
                record.put("ActivityType", "Won");
                event.setRecord(record);
                Item item = createItem(event);
                events.add(item);
            }
            dynamoItemService.batchWrite(tableName, events);
        }
        TimeLineProxy spiedTimelineProxy = spy(new TimeLineProxy());
        TimeLine tl = new TimeLine();
        tl.setTimelineId(TIMELINE_ID);
        doReturn(tl).when(spiedTimelineProxy).findByEntity(TENANT_ID, BusinessEntity.Account);
        ((ActivityTimelineQueryServiceImpl) activityTimelineQueryService).setTimeLineProxy(spiedTimelineProxy);

        DataCollectionProxy spiedDCProxy = spy(new DataCollectionProxy());
        DataCollectionStatus dcs = new DataCollectionStatus();
        dcs.setDetail(new DataCollectionStatusDetail());
        dcs.getDetail().setTimelineVersionMap(new HashMap<>());
        dcs.getDetail().getTimelineVersionMap().put(TIMELINE_ID, VERSION_ID);
        doReturn(dcs).when(spiedDCProxy).getOrCreateDataCollectionStatus(TENANT_ID, null);
        ((ActivityTimelineQueryServiceImpl) activityTimelineQueryService).setDataCollectionProxy(spiedDCProxy);

    }

    @Test(groups = "functional", enabled = false)
    public void testTimelineQuery() {
        ((ActivityTimelineQueryServiceImpl) activityTimelineQueryService)
                .setTableName("TimelineQueryServiceImplTestNG_dev_jlmehta");
        ActivityTimelineQuery activityTimelineQuery = new ActivityTimelineQuery();
        activityTimelineQuery.setMainEntity(BusinessEntity.Account);
        activityTimelineQuery.setEntityId("1");
        activityTimelineQuery.setStartTimeStamp(Instant.now().minus(90, ChronoUnit.DAYS));
        activityTimelineQuery.setEndTimeStamp(Instant.now());
        DataPage result = activityTimelineQueryService.getData(TENANT_ID, null, activityTimelineQuery);
        Assert.assertNotNull(result);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result.getData()));
    }

    @Test(groups = "functional")
    public void testTimelineQuery1() {
        TimeLineProxy spiedTimelineProxy = spy(new TimeLineProxy());
        TimeLine tl = new TimeLine();
        tl.setTimelineId("slin_tlimeline_1_Account360");
        doReturn(tl).when(spiedTimelineProxy).findByEntity("slin_tlimeline_1", BusinessEntity.Account);
        ((ActivityTimelineQueryServiceImpl) activityTimelineQueryService).setTimeLineProxy(spiedTimelineProxy);

        DataCollectionProxy spiedDCProxy = spy(new DataCollectionProxy());
        DataCollectionStatus dcs = new DataCollectionStatus();
        dcs.setDetail(new DataCollectionStatusDetail());
        dcs.getDetail().setTimelineVersionMap(new HashMap<>());
        dcs.getDetail().getTimelineVersionMap().put("slin_tlimeline_1_Account360", "1587775242801");
        doReturn(dcs).when(spiedDCProxy).getOrCreateDataCollectionStatus("slin_tlimeline_1", null);
        ((ActivityTimelineQueryServiceImpl) activityTimelineQueryService).setDataCollectionProxy(spiedDCProxy);

        ActivityTimelineQuery activityTimelineQuery = new ActivityTimelineQuery();
        activityTimelineQuery.setMainEntity(BusinessEntity.Account);
        activityTimelineQuery.setEntityId("due0j2mlehd7zv1u");
        activityTimelineQuery.setStartTimeStamp(Instant.now().minus(90, ChronoUnit.DAYS));
        activityTimelineQuery.setEndTimeStamp(Instant.now());

        DataPage result = activityTimelineQueryService.getData("slin_tlimeline_1", null, activityTimelineQuery);
        Assert.assertNotNull(result);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result.getData()));
    }

    private Item createItem(GenericTimeseriesRecord event) {
        Item item = new Item();
        item.with(GenericTimeseriesRecord.PARTITION_KEY_ATTR, event.getPartitionKey());
        item.with(GenericTimeseriesRecord.RANGE_KEY_ATTR, event.getRangeKey());
        item.with(GenericTimeseriesRecord.RECORD_ATTR, event.getRecord());
        return item;
    }
}
