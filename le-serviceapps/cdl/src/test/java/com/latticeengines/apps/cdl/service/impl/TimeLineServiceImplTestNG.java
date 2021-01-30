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
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.TimeLineService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.EventFieldExtractor;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({SimpleRetryListener.class})
public class TimeLineServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TimeLineServiceImplTestNG.class);

    private static final String EVENT_TIME = "EventTime";
    private static final String EVENT_TYPE = "EventType";
    private static final String MOTION = "Motion";
    private static final String TIMELINE_NAME = "timelineName1";

    @Inject
    private TimeLineService timeLineService;

    private TimeLine timeline;
    private TimeLine created;
    private RetryTemplate retry;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        timeline = setupTestTimeline();
        retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testCreate() {
        created = timeLineService.createOrUpdateTimeLine(mainCustomerSpace, timeline);
        Assert.assertNotNull(created.getPid());
        Assert.assertNotNull(created.getTimelineId());
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testFetch() {
        TimeLine fetchedByPid = timeLineService.findByPid(mainCustomerSpace, created.getPid());
        Assert.assertEquals(fetchedByPid.getName(), TIMELINE_NAME);
        Assert.assertTrue(fetchedByPid.getEventMappings().containsKey(AtlasStream.StreamType.MarketingActivity.name()));
        Assert.assertTrue(fetchedByPid.getEventMappings().get(AtlasStream.StreamType.MarketingActivity.name()).containsKey(MOTION));
        Assert.assertEquals(fetchedByPid.getEventMappings().get(AtlasStream.StreamType.MarketingActivity.name()).get(MOTION).getMappingValue(), InterfaceName.ActivityType.name());
        TimeLine fetchedByTimelineId = timeLineService.findByTimelineId(mainCustomerSpace, created.getTimelineId());
        Assert.assertEquals(fetchedByTimelineId.getTimelineId(), created.getTimelineId());
    }

    @Test(groups = "functional", dependsOnMethods = "testFetch", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testDelete() {
        timeLineService.delete(mainCustomerSpace, created);
        TimeLine fetched = timeLineService.findByTimelineId(mainCustomerSpace, created.getTimelineId());
        Assert.assertNull(fetched);
    }

    @Test(groups = "functional", dependsOnMethods = "testDelete")
    public void testDefault() {
        timeLineService.createDefaultTimeLine(mainCustomerSpace);
        AtomicReference<List<TimeLine>> createdAtom = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom.set(timeLineService.findByTenant(mainCustomerSpace));
            Assert.assertEquals(createdAtom.get().size(), 2);
            return true;
        });
        List<TimeLine> timeLines = createdAtom.get();
        Assert.assertEquals(timeLines.size(), 2);
        AtomicReference<TimeLine> createdAtom1 = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom1.set(timeLineService.findByTenantAndEntity(mainCustomerSpace, BusinessEntity.Account));
            Assert.assertNotNull(createdAtom1.get());
            return true;
        });
        TimeLine created = createdAtom1.get();
        Assert.assertEquals(created.getName(), TimeLineStoreUtils.ACCOUNT360_TIMELINE_NAME);
    }

    private TimeLine setupTestTimeline() {
        timeline = new TimeLine();
        timeline.setName(TIMELINE_NAME);
        timeline.setTimelineId(String.format("%s_%s", CustomerSpace.shortenCustomerSpace(mainCustomerSpace), TIMELINE_NAME));
        timeline.setTenant(mainTestTenant);
        timeline.setEntity(BusinessEntity.Account.name());
        timeline.setStreamTypes(Arrays.asList(AtlasStream.StreamType.WebVisit, AtlasStream.StreamType.MarketingActivity));
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

        timeline.setEventMappings(mappingMap);
        return timeline;
    }
}
