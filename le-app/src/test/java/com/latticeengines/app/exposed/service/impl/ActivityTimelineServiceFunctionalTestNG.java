package com.latticeengines.app.exposed.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.InputStream;

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
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.DataPage;
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
        doReturn(generateTestData("com/latticeengines/app/exposed/controller/test-activity-timeline-insight-data.json"))
                .when(spiedActivityProxy).getData(any(String.class), eq(null), any(ActivityTimelineQuery.class));
        ((ActivityTimelineServiceImpl) activityTimelineService).setActivityProxy(spiedActivityProxy);

        DataLakeService spiedDataLakeService = spy(new DataLakeServiceImpl(null));
        doReturn(TEST_ACCOUNT_ID).when(spiedDataLakeService).getInternalAccountId(TEST_ACCOUNT_ID, null);
        ((ActivityTimelineServiceImpl) activityTimelineService).setDataLakeService(spiedDataLakeService);
    }

    @Test(groups = "functional")
    public void testGetMetrics(){
        int result = activityTimelineService.getNewWebActivitiesCount(TEST_ACCOUNT_ID,null,null);
        Assert.assertEquals(result,139);

        result = activityTimelineService.getIdentifiedContactsCount(TEST_ACCOUNT_ID,null,null);
        Assert.assertEquals(result,0);

        result = activityTimelineService.getNewEngagementsCount(TEST_ACCOUNT_ID,null,null);
        Assert.assertEquals(result,207);

        result = activityTimelineService.getNewOpportunitiesCount(TEST_ACCOUNT_ID,null,null);
        Assert.assertEquals(result,97);
    }

    private DataPage generateTestData(String filePath) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream dataStream = classLoader.getResourceAsStream(filePath);
        return JsonUtils.deserialize(dataStream, DataPage.class);
    }
}
