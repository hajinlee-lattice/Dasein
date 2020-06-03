package com.latticeengines.app.exposed.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.service.ActivityTimelineService;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.objectapi.ActivityProxy;
import com.latticeengines.proxy.objectapi.ActivityProxyImpl;

public class ActivityTimelineServiceFunctionalTestNG extends AppFunctionalTestNGBase {

    @Inject
    private ActivityTimelineService activityTimelineService;

    private final String TEST_ACCOUNT_ID = "lck9awpmxtg1kqc4";

    @BeforeClass(groups = "functional")
    private void setup() {
        setupTestEnvironmentWithOneTenant();
        ActivityProxy spiedActivityProxy = spy(new ActivityProxyImpl());
        doReturn(generateTestData("com/latticeengines/app/exposed/controller/test-activity-timeline-data.json"))
                .when(spiedActivityProxy).getData(any(String.class), eq(null), any(ActivityTimelineQuery.class));
        ((ActivityTimelineServiceImpl) activityTimelineService).setActivityProxy(spiedActivityProxy);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream dataStream = classLoader
                .getResourceAsStream("com/latticeengines/app/exposed/controller/test-steam-dimension-data.json");
        Map<String, DimensionMetadata> data = JsonUtils.convertMap(JsonUtils.deserialize(dataStream, Map.class),
                String.class, DimensionMetadata.class);
        ActivityStoreProxy spiedActivityStoreProxy = spy(new ActivityStoreProxy(""));
        doReturn(data).when(spiedActivityStoreProxy).getDimensionMetadataInStream(any(String.class), any(String.class),
                eq(null));
        ((ActivityTimelineServiceImpl) activityTimelineService).setActivityStoreProxy(spiedActivityStoreProxy);

        DataLakeService spiedDataLakeService = spy(new DataLakeServiceImpl(null));
        doReturn(TEST_ACCOUNT_ID).when(spiedDataLakeService).getInternalAccountId(TEST_ACCOUNT_ID, null);
        doReturn(generateTestData("com/latticeengines/app/exposed/controller/test-contacts-data.json"))
                .when(spiedDataLakeService).getAllContactsByAccountId(TEST_ACCOUNT_ID, null);
        ((ActivityTimelineServiceImpl) activityTimelineService).setDataLakeService(spiedDataLakeService);
    }

    private DataPage generateTestData(String filePath) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream dataStream = classLoader.getResourceAsStream(filePath);
        return JsonUtils.deserialize(dataStream, DataPage.class);
    }

    @Test(groups = "functional")
    public void testContactReport() {
        DataPage result = activityTimelineService.getAccountAggregationReportByContact(TEST_ACCOUNT_ID, "P90D", null);
        Assert.assertNotNull(result);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result.getData()));
        Map<String, Long> expectedResults = expectedContactReportResults();
        for (Map<String, Object> item : result.getData()) {
            Assert.assertTrue(MapUtils.isNotEmpty(item));
            Assert.assertEquals(expectedResults.getOrDefault(item.get("contactName"), -1L), item.get("ActivityCount"),
                    "Failed for :" + item.get("contactName"));
        }
    }

    @Test(groups = "functional")
    public void testProductInterestReport() {
        DataPage result = activityTimelineService.getAccountAggregationReportByProductInterest(TEST_ACCOUNT_ID, "P90D",
                null);
        Assert.assertNotNull(result);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result.getData()));
        Map<String, Long> expectedResults = expectedProductPathReportResults();
        for (Map<String, Object> item : result.getData()) {
            Assert.assertTrue(MapUtils.isNotEmpty(item));
            Assert.assertEquals(expectedResults.getOrDefault(item.get("PathPatternName"), -1L),
                    item.get("ActivityCount"), "Failed for :" + item.get("PathPatternName"));
        }
    }

    @Test(groups = "functional")
    public void testActivityEventTypeReport() {
        DataPage result = activityTimelineService.getAccountAggregationReportByEventType(TEST_ACCOUNT_ID, "P90D", null);
        Assert.assertNotNull(result);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result.getData()));
        Map<String, Long> expectedResults = expectedEventReportResults();
        for (Map<String, Object> item : result.getData()) {
            Assert.assertTrue(MapUtils.isNotEmpty(item));
            Assert.assertEquals(expectedResults.getOrDefault(item.get("eventType"), -1L), item.get("ActivityCount"),
                    "Failed for :" + item.get("eventType"));
        }
    }

    private Map<String, Long> expectedContactReportResults() {
        Map<String, Long> expectedResults = new HashMap<>();
        expectedResults.put("DEBBIE KALISZEWSKI", 6L);
        expectedResults.put("MASHOUF SHAYKH PH.D.", 6L);
        expectedResults.put("FRANK INDOF", 3L);
        expectedResults.put("VALERIE WEISS", 6L);
        expectedResults.put("CATHERINE YOGERST", 2L);
        expectedResults.put("Anonymous", 29L);
        expectedResults.put("All", 52L);
        return expectedResults;
    }

    private Map<String, Long> expectedEventReportResults() {
        Map<String, Long> expectedResults = new HashMap<>();
        expectedResults.put("Opportunity Update", 18L);
        expectedResults.put("Page Visit", 11L);
        expectedResults.put("Email Bounced Soft", 11L);
        expectedResults.put("WebVisit", 6L);
        expectedResults.put("Email Bounced", 4L);
        expectedResults.put("Get New Contact", 1L);
        expectedResults.put("from WeChat", 1L);
        expectedResults.put("All", 52L);
        return expectedResults;
    }

    private Map<String, Long> expectedProductPathReportResults() {
        Map<String, Long> expectedResults = new HashMap<>();
        expectedResults.put("Library", 2L);
        expectedResults.put("All Applications", 2L);
        expectedResults.put("Product Page", 1L);
        expectedResults.put("Products and industries", 1L);
        expectedResults.put("Register", 1L);
        expectedResults.put("About-us", 1L);
        expectedResults.put("All", 8L);
        return expectedResults;
    }
}
