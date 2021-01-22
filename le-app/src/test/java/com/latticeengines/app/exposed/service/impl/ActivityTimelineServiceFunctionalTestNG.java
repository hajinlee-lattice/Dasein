package com.latticeengines.app.exposed.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.service.ActivityTimelineService;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimelineMetrics;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.objectapi.ActivityProxy;
import com.latticeengines.proxy.objectapi.ActivityProxyImpl;

public class ActivityTimelineServiceFunctionalTestNG extends AppFunctionalTestNGBase {

    @Inject
    private ActivityTimelineService activityTimelineService;

    private final String TEST_ACCOUNT_ID = "0014P000028BlGMQA0";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();

        ActivityProxy spiedActivityProxy = spy(new ActivityProxyImpl());
        doReturn(generateTestData("com/latticeengines/app/exposed/controller/test-activity-timeline-insight-data.json",
                DataPage.class)).when(spiedActivityProxy).getData(any(String.class), eq(null),
                        any(ActivityTimelineQuery.class));
        ((ActivityTimelineServiceImpl) activityTimelineService).setActivityProxy(spiedActivityProxy);

        DataLakeService spiedDataLakeService = spy(new DataLakeServiceImpl(null));
        doReturn(TEST_ACCOUNT_ID).when(spiedDataLakeService).getInternalAccountId(TEST_ACCOUNT_ID, null);
        doReturn(generateTestData("com/latticeengines/app/exposed/controller/test-contacts-data.json", DataPage.class))
                .when(spiedDataLakeService).getAllContactsByAccountId(TEST_ACCOUNT_ID, null);
        ((ActivityTimelineServiceImpl) activityTimelineService).setDataLakeService(spiedDataLakeService);

        ActivityStoreProxy spiedActivityStoreProxy = spy(
                new ActivityStoreProxy(PropertyUtils.getProperty("common.microservice.url")));
        List<?> raws = generateTestData(
                "com/latticeengines/app/exposed/controller/test-journey-stage-configurations.json", List.class);
        doReturn(JsonUtils.convertList(raws, JourneyStage.class)).when(spiedActivityStoreProxy)
                .getJourneyStages(any(String.class));
        List<AtlasStream> streams = Arrays.asList(
                new AtlasStream.Builder().withStreamType(AtlasStream.StreamType.WebVisit).build(),
                new AtlasStream.Builder().withStreamType(AtlasStream.StreamType.MarketingActivity).build(),
                new AtlasStream.Builder().withStreamType(AtlasStream.StreamType.Opportunity).build());
        doReturn(streams).when(spiedActivityStoreProxy).getStreams(any(String.class));
        ((ActivityTimelineServiceImpl) activityTimelineService).setActivityStoreProxy(spiedActivityStoreProxy);

        Path cdlPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(mainTestTenant.getId()), "CDL");
        Path fakeCurrentDatePath = cdlPath.append("FakeCurrentDate");
        CamilleEnvironment.getCamille().upsert(fakeCurrentDatePath, new Document("2020-08-12"),
                ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    @Test(groups = "functional")
    public void testGetMetrics() {
        List<ActivityTimelineMetrics> metrics = activityTimelineService.getActivityTimelineMetrics(TEST_ACCOUNT_ID,
                null, null);

        Assert.assertEquals(metrics.get(0).getLabel(), "New Activities");
        Assert.assertEquals(String.valueOf(metrics.get(0).getMessage()), "139");
        Assert.assertEquals(metrics.get(0).getDescription(), " in last 10 days");
        Assert.assertEquals(metrics.get(0).getContext(), "Total number of web activity in the last 10 days");

        Assert.assertEquals(metrics.get(1).getLabel(), "New Contacts");
        Assert.assertEquals(String.valueOf(metrics.get(1).getMessage()), "0");
        Assert.assertEquals(metrics.get(1).getDescription(), "");
        Assert.assertEquals(metrics.get(1).getContext(), "Total number of new contacts in the last 10 days");

        Assert.assertEquals(metrics.get(2).getLabel(), "New Engagements");
        Assert.assertEquals(String.valueOf(metrics.get(2).getMessage()), "207");
        Assert.assertEquals(metrics.get(2).getDescription(), " in last 10 days");
        Assert.assertEquals(metrics.get(2).getContext(), "Total number of engagements in the last 10 days");

        Assert.assertEquals(metrics.get(3).getLabel(), "New Opportunities");
        Assert.assertEquals(String.valueOf(metrics.get(3).getMessage()), "2");
        Assert.assertEquals(metrics.get(3).getDescription(), " in last 10 days");
        Assert.assertEquals(metrics.get(3).getContext(), "Number of present open opportunities in the last 10 days");

        Assert.assertEquals(metrics.get(4).getLabel(), "Account Intent");
        Assert.assertEquals(String.valueOf(metrics.get(4).getMessage()), "Buying");
        Assert.assertEquals(metrics.get(4).getDescription(), "");
        Assert.assertEquals(metrics.get(4).getContext(), "Account intent in the last 10 days");

    }

    @Test(groups = "functional", dependsOnMethods = "testGetMetrics")
    public void testGetActivityWithBackStage() {
        List<Map<String, Object>> dataWithbackStage = activityTimelineService
                .getAccountActivities(TEST_ACCOUNT_ID, null, null, //
                        Arrays.stream(AtlasStream.StreamType.values()).collect(Collectors.toSet()), null)
                .getData();
        Assert.assertEquals(dataWithbackStage.size(), 309);
        Map<String, Object> backStageEvent = dataWithbackStage.get(dataWithbackStage.size() - 1);
        Assert.assertEquals(backStageEvent.get("StreamType"), "JourneyStage");
        Assert.assertEquals(backStageEvent.get("Detail1"), "Engaged");
        Assert.assertEquals(backStageEvent.get("EventTimestamp"), 1589414400000L);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetActivityWithBackStage")
    public void testGetActivityWithBackStage1() throws Exception {
        // setup
        Path cdlPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(mainTestTenant.getId()), "CDL");
        Path fakeCurrentDatePath = cdlPath.append("FakeCurrentDate");
        CamilleEnvironment.getCamille().upsert(fakeCurrentDatePath, new Document("2020-06-12"),
                ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Instant cutoffTimeStamp = LocalDate.parse("2020-06-12", DateTimeFormatter.ISO_DATE).atStartOfDay(ZoneOffset.UTC)
                .toOffsetDateTime().toInstant();
        ActivityProxy spiedActivityProxy = spy(new ActivityProxyImpl());
        doReturn(new DataPage(generateTestData(
                "com/latticeengines/app/exposed/controller/test-activity-timeline-insight-data.json", DataPage.class)
                        .getData().stream()
                        .filter(event -> (Long) event.get(InterfaceName.EventTimestamp.name()) >= cutoffTimeStamp
                                .toEpochMilli())
                        .collect(Collectors.toList()))).when(spiedActivityProxy).getData(any(String.class), eq(null),
                                any(ActivityTimelineQuery.class));
        ((ActivityTimelineServiceImpl) activityTimelineService).setActivityProxy(spiedActivityProxy);

        // Test
        List<Map<String, Object>> dataWithbackStage = activityTimelineService
                .getAccountActivities(TEST_ACCOUNT_ID, null, null, //
                        Arrays.stream(AtlasStream.StreamType.values()).collect(Collectors.toSet()), null)
                .getData();
        Assert.assertEquals(dataWithbackStage.size(), 161);
        Map<String, Object> backStageEvent = dataWithbackStage.get(dataWithbackStage.size() - 1);
        Assert.assertEquals(backStageEvent.get("StreamType"), "JourneyStage");
        Assert.assertEquals(backStageEvent.get("Detail1"), "Dark");
        Assert.assertEquals(backStageEvent.get("EventTimestamp"), 1584144000000L);
    }

    private <T> T generateTestData(String filePath, Class<T> clazz) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream dataStream = classLoader.getResourceAsStream(filePath);
        return JsonUtils.deserialize(dataStream, clazz);
    }
}
