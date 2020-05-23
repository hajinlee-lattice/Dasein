package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.TimeLineService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.EventFieldExtractor;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class TimeLineServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TimeLineServiceImplTestNG.class);

    private static final String EVENT_TIME = "EventTime";
    private static final String EVENT_TYPE = "EventType";
    private static final String MOTION = "Motion";

    @Inject
    private TimeLineService timeLineService;

    private TimeLine timeLine1;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCRUD() {
        String timelineName1 = "timelineName1";

        timeLine1 = new TimeLine();
        timeLine1.setName(timelineName1);
        timeLine1.setTimelineId(String.format("%s_%s", CustomerSpace.shortenCustomerSpace(mainCustomerSpace), timelineName1));
        timeLine1.setTenant(mainTestTenant);
        timeLine1.setEntity(BusinessEntity.Account.name());
        timeLine1.setStreamTypes(Arrays.asList(AtlasStream.StreamType.WebVisit, AtlasStream.StreamType.MarketingActivity));
        Map<String, Map<String, EventFieldExtractor>> mappingMap = new HashMap<>();

        Map<String, EventFieldExtractor> eventTypeExtractorMapForMarketing = new HashMap<>();
        EventFieldExtractor eventTimeExtractorForMarketing = new EventFieldExtractor();
        eventTimeExtractorForMarketing.setMappingType(EventFieldExtractor.MappingType.Attribute);
        eventTimeExtractorForMarketing.setMappingValue(InterfaceName.ActivityDate.name());
        eventTypeExtractorMapForMarketing.put(EVENT_TIME, eventTimeExtractorForMarketing);
        EventFieldExtractor eventFieldExtractorForMarketing = new EventFieldExtractor();
        eventFieldExtractorForMarketing.setMappingType(EventFieldExtractor.MappingType.Attribute);
        eventFieldExtractorForMarketing.setMappingValue(InterfaceName.ActivityType.name());
        eventTypeExtractorMapForMarketing.put(EVENT_TYPE, eventFieldExtractorForMarketing);
        EventFieldExtractor motionExtractor = new EventFieldExtractor();
        motionExtractor.setMappingType(EventFieldExtractor.MappingType.AttributeWithMapping);
        motionExtractor.setMappingValue(InterfaceName.ActivityType.name());
        Map<String, String> mappings = new HashMap<>();
        mappings.put("EmailOpen", "Outbound");
        mappings.put("Subscribe", "Inbound");
        motionExtractor.setMappingMap(mappings);
        eventTypeExtractorMapForMarketing.put(MOTION, motionExtractor);

        Map<String, EventFieldExtractor> eventTypeExtractorMapForWebVisit = new HashMap<>();
        EventFieldExtractor eventTimeExtractorForWebVisit = new EventFieldExtractor();
        eventTimeExtractorForWebVisit.setMappingType(EventFieldExtractor.MappingType.Attribute);
        eventTimeExtractorForWebVisit.setMappingValue(InterfaceName.WebVisitDate.name());
        eventTypeExtractorMapForWebVisit.put(EVENT_TIME, eventTimeExtractorForWebVisit);
        EventFieldExtractor eventFieldExtractorForWebVisit = new EventFieldExtractor();
        eventFieldExtractorForWebVisit.setMappingType(EventFieldExtractor.MappingType.Constant);
        eventFieldExtractorForWebVisit.setMappingValue("WebVisit");
        eventTypeExtractorMapForWebVisit.put(EVENT_TYPE, eventFieldExtractorForWebVisit);

        mappingMap.put(AtlasStream.StreamType.MarketingActivity.name(), eventTypeExtractorMapForMarketing);
        mappingMap.put(AtlasStream.StreamType.WebVisit.name(), eventTypeExtractorMapForWebVisit);

        timeLine1.setEventMappings(mappingMap);
        TimeLine created = timeLineService.createOrUpdateTimeLine(mainCustomerSpace, timeLine1);
        Assert.assertNotNull(created.getPid());
        Assert.assertNotNull(created.getTimelineId());

        List<TimeLine> timeLineList = timeLineService.findByTenant(mainCustomerSpace);
        Assert.assertEquals(timeLineList.size(), 1);
        created = timeLineList.get(0);
        log.info("timeline is {}.", JsonUtils.serialize(created));
        Assert.assertEquals(created.getName(), timelineName1);
        Assert.assertTrue(created.getEventMappings().containsKey(AtlasStream.StreamType.MarketingActivity.name()));
        Assert.assertTrue(created.getEventMappings().get(AtlasStream.StreamType.MarketingActivity.name()).containsKey(MOTION));
        Assert.assertEquals(created.getEventMappings().get(AtlasStream.StreamType.MarketingActivity.name()).get(MOTION).getMappingValue(), InterfaceName.ActivityType.name());
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
        AtomicReference<TimeLine> createdAtom = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom.set(timeLineService.findByTimelineId(mainCustomerSpace, timeLine1.getTimelineId()));
            Assert.assertNotNull(createdAtom.get());
            return true;
        });
        created = createdAtom.get();
        Assert.assertEquals(created.getTimelineId(), timeLine1.getTimelineId());
    }
}
