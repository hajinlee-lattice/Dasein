package com.latticeengines.app.exposed.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.service.ActivityTimelineService;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.objectapi.ActivityProxy;
import com.latticeengines.proxy.objectapi.ActivityProxyImpl;

public class ActivityTimelineServiceFunctionalTestNG extends AppFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ActivityTimelineServiceFunctionalTestNG.class);

    @Inject
    private ActivityTimelineService activityTimelineService;

    private final String TEST_ACCOUNT_ID = "0014P000028BlGMQA0";

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithOneTenant();

        ActivityProxy spiedActivityProxy = spy(new ActivityProxyImpl());
        doReturn(generateTestData("com/latticeengines/app/exposed/controller/test-activity-timeline-insight-data.json",
                DataPage.class)).when(spiedActivityProxy).getData(any(String.class), eq(null),
                        any(ActivityTimelineQuery.class));
        ((ActivityTimelineServiceImpl) activityTimelineService).setActivityProxy(spiedActivityProxy);

        DataLakeService spiedDataLakeService = spy(new DataLakeServiceImpl(null));
        doReturn(TEST_ACCOUNT_ID).when(spiedDataLakeService).getInternalAccountId(TEST_ACCOUNT_ID, null);
        ((ActivityTimelineServiceImpl) activityTimelineService).setDataLakeService(spiedDataLakeService);

        ActivityStoreProxy spiedActivityStoreProxy = spy(
                new ActivityStoreProxy(PropertyUtils.getProperty("common.microservice.url")));
        List<?> raws = generateTestData(
                "com/latticeengines/app/exposed/controller/test-journey-stage-configurations.json", List.class);
        doReturn(JsonUtils.convertList(raws, JourneyStage.class)).when(spiedActivityStoreProxy)
                .getJourneyStages(any(String.class));
        ((ActivityTimelineServiceImpl) activityTimelineService).setActivityStoreProxy(spiedActivityStoreProxy);
    }

    @Test(groups = "functional")
    public void testGetMetrics() {
        Map<String, Integer> metrics = activityTimelineService.getActivityTimelineMetrics(TEST_ACCOUNT_ID, null, null);
        Assert.assertEquals(metrics.get("newActivities").intValue(), 139);

        Assert.assertEquals(metrics.get("newIdentifiedContacts").intValue(), 0);

        Assert.assertEquals(metrics.get("newEngagements").intValue(), 207);

        Assert.assertEquals(metrics.get("newOpportunities").intValue(), 97);
    }

    @Test(groups = "functional")
    public void testGetActivityWithBackStage() {
        List<Map<String, Object>> dataWithbackStage = activityTimelineService
                .getCompleteTimelineActivities(TEST_ACCOUNT_ID, null, null, null).getData();
        Map<String, Object> backStageEvent = dataWithbackStage.get(dataWithbackStage.size() - 1);
        Assert.assertTrue(backStageEvent.get("StreamType").equals("JourneyStage"));
        Assert.assertTrue(backStageEvent.get("Detail1").equals("Dark"));
        Assert.assertTrue(backStageEvent.get("EventTimestamp").equals(1595030400000L));
    }

    private <T> T generateTestData(String filePath, Class<T> clazz) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream dataStream = classLoader.getResourceAsStream(filePath);
        return JsonUtils.deserialize(dataStream, clazz);
    }
}
