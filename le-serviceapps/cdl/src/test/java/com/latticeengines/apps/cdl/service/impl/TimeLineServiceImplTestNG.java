package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.TimeLineService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.EventTypeExtractor;
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
    public void testCRUD() throws Exception {
        String timelineName1 = "timelineName1";

        timeLine1 = new TimeLine();
        timeLine1.setName(timelineName1);
        timeLine1.setTenant(mainTestTenant);
        timeLine1.setEntity(BusinessEntity.Account.name());
        timeLine1.setStreamTypes(Arrays.asList(AtlasStream.StreamType.WebVisit, AtlasStream.StreamType.MarketingActivity));
        Map<AtlasStream.StreamType, Map<String, EventTypeExtractor>> mappingMap = new HashMap<>();

        Map<String, EventTypeExtractor> eventTypeExtractorMapForMarketing = new HashMap<>();
        EventTypeExtractor eventTimeExtractorForMarketing = new EventTypeExtractor();
        eventTimeExtractorForMarketing.setMappingType(EventTypeExtractor.MappingType.Attribute);
        eventTimeExtractorForMarketing.setMappingValue(InterfaceName.ActivityDate.name());
        eventTypeExtractorMapForMarketing.put(EVENT_TIME, eventTimeExtractorForMarketing);
        EventTypeExtractor eventTypeExtractorForMarketing = new EventTypeExtractor();
        eventTypeExtractorForMarketing.setMappingType(EventTypeExtractor.MappingType.Attribute);
        eventTypeExtractorForMarketing.setMappingValue(InterfaceName.ActivityType.name());
        eventTypeExtractorMapForMarketing.put(EVENT_TYPE, eventTypeExtractorForMarketing);
        EventTypeExtractor motionExtractor = new EventTypeExtractor();
        motionExtractor.setMappingType(EventTypeExtractor.MappingType.AttributeWithMapping);
        motionExtractor.setMappingValue(InterfaceName.ActivityType.name());
        Map<String, String> mappings = new HashMap<>();
        mappings.put("EmailOpen", "Outbound");
        mappings.put("Subscribe", "Inbound");
        motionExtractor.setMappingMap(mappings);
        eventTypeExtractorMapForMarketing.put(MOTION, motionExtractor);

        Map<String, EventTypeExtractor> eventTypeExtractorMapForWebVisit = new HashMap<>();
        EventTypeExtractor eventTimeExtractorForWebVisit = new EventTypeExtractor();
        eventTimeExtractorForWebVisit.setMappingType(EventTypeExtractor.MappingType.Attribute);
        eventTimeExtractorForWebVisit.setMappingValue(InterfaceName.WebVisitDate.name());
        eventTypeExtractorMapForWebVisit.put(EVENT_TIME, eventTimeExtractorForWebVisit);
        EventTypeExtractor eventTypeExtractorForWebVisit = new EventTypeExtractor();
        eventTypeExtractorForWebVisit.setMappingType(EventTypeExtractor.MappingType.Constant);
        eventTypeExtractorForWebVisit.setMappingValue("WebVisit");
        eventTypeExtractorMapForWebVisit.put(EVENT_TYPE, eventTypeExtractorForWebVisit);

        mappingMap.put(AtlasStream.StreamType.MarketingActivity, eventTypeExtractorMapForMarketing);
        mappingMap.put(AtlasStream.StreamType.WebVisit, eventTypeExtractorMapForWebVisit);

        timeLine1.setEventMappings(mappingMap);
        TimeLine created = timeLineService.createOrUpdateTimeLine(mainCustomerSpace, timeLine1);
        Assert.assertNotNull(created.getPid());
        Assert.assertNotNull(created.getTimelineId());

        List<TimeLine> timeLineList = timeLineService.findByTenant(mainCustomerSpace);
        Assert.assertEquals(timeLineList.size(), 1);
        created = timeLineList.get(0);
        log.info("timeline is {}.", JsonUtils.serialize(created));
        Assert.assertEquals(created.getName(), timelineName1);
        Assert.assertTrue(created.getEventMappings().containsKey(AtlasStream.StreamType.MarketingActivity));
        Assert.assertTrue(created.getEventMappings().get(AtlasStream.StreamType.MarketingActivity).containsKey(MOTION));
        Assert.assertEquals(created.getEventMappings().get(AtlasStream.StreamType.MarketingActivity).get(MOTION).getMappingValue(), InterfaceName.ActivityType.name());

        String timeline_id = created.getTimelineId();
        created = timeLineService.findByTimelineId(mainCustomerSpace, timeline_id);
        Assert.assertEquals(created.getTimelineId(), timeline_id);
    }
}
