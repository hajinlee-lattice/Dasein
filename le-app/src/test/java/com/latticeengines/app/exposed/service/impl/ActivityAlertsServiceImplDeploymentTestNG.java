package com.latticeengines.app.exposed.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.repository.datadb.ActivityAlertRepository;
import com.latticeengines.app.exposed.service.ActivityAlertsService;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.app.testframework.AppDeploymentTestNGBase;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;
import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants;
import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;
import com.latticeengines.domain.exposed.cdl.activitydata.ActivityAlert;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

public class ActivityAlertsServiceImplDeploymentTestNG extends AppDeploymentTestNGBase {

    @Inject
    private ActivityAlertRepository activityAlertRepository;

    @Inject
    private ActivityMetricsProxy activityMetricsProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Inject
    private ActivityAlertsService activityAlertsService;

    private final DataCollection.Version DATA_COLLECTION_VERSION = DataCollection.Version.Blue;
    private final String TEST_ACCOUNT_ID = "v5k5xq52updfo67n";
    private final String TEST_ALERT_VERSION = "AlertVersion";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ACCOUNT360.getName(), true);
        setupTestEnvironmentWithOneTenant(featureFlagMap);
        setupDataCollection();
        activityAlertRepository.saveAll(generatePeopleAlerts());
        activityAlertRepository.saveAll(generateProductsAlerts());
        activityStoreProxy.generateDefaultActivityAlertsConfiguration(mainTestCustomerSpace.getTenantId());

        DataLakeService spiedDataLakeService = spy(new DataLakeServiceImpl(null));
        doReturn(TEST_ACCOUNT_ID).when(spiedDataLakeService).getInternalAccountId(TEST_ACCOUNT_ID, null);
        ((ActivityAlertsServiceImpl) activityAlertsService).setDataLakeService(spiedDataLakeService);
    }

    @Test(groups = "deployment")
    public void testActivityTimelineMetrics() {
        DataPage data = activityAlertsService.findActivityAlertsByAccountAndCategory(
                mainTestCustomerSpace.getTenantId(), TEST_ACCOUNT_ID, AlertCategory.PEOPLE, 3, null);
        Assert.assertNotNull(data);
        Assert.assertEquals(data.getData().size(), 3);

        data = activityAlertsService.findActivityAlertsByAccountAndCategory(mainTestCustomerSpace.getTenantId(),
                TEST_ACCOUNT_ID, AlertCategory.PRODUCTS, 3, null);
        Assert.assertNotNull(data);
        Assert.assertEquals(data.getData().size(), 3);
    }

    private void setupDataCollection() {
        DataCollectionStatus dcs = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestCustomerSpace.getTenantId(), DATA_COLLECTION_VERSION);
        dcs.setVersion(DATA_COLLECTION_VERSION);
        dcs.setActivityAlertVersion(TEST_ALERT_VERSION);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainTestCustomerSpace.getTenantId(), dcs,
                dcs.getVersion());
    }

    private List<ActivityAlert> generatePeopleAlerts() {
        List<ActivityAlert> alerts = new ArrayList<>();
        Tenant t = mainTestTenant;
        // record 1
        ActivityAlert record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.SHOWN_INTENT);
        record.setEntityId(TEST_ACCOUNT_ID);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(new Date());
        record.setVersion(TEST_ALERT_VERSION);
        record.setCategory(AlertCategory.PEOPLE);

        Map<String, Object> data = new HashMap<>();
        Instant end = Instant.now();
        Instant start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        Map<String, Object> alertData = new HashMap<>();
        alertData.put("NumBuyIntents", 2);
        alertData.put("NumResearchIntents", 5);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 2
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.SHOWN_INTENT);
        record.setEntityId(TEST_ACCOUNT_ID);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(Date.from(Instant.now().minus(10, ChronoUnit.HOURS)));
        record.setVersion(TEST_ALERT_VERSION);
        record.setCategory(AlertCategory.PEOPLE);

        data = new HashMap<>();
        end = Instant.now().minus(1, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        alertData.put("NumBuyIntents", 2);
        alertData.put("NumResearchIntents", 0);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 3
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.SHOWN_INTENT);
        record.setEntityId(TEST_ACCOUNT_ID);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(Date.from(Instant.now().minus(1, ChronoUnit.DAYS)));
        record.setVersion(TEST_ALERT_VERSION);
        record.setCategory(AlertCategory.PEOPLE);

        data = new HashMap<>();
        end = Instant.now().minus(7, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        alertData.put("NumBuyIntents", 0);
        alertData.put("NumResearchIntents", 3);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 4
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.SHOWN_INTENT);
        record.setEntityId(TEST_ACCOUNT_ID);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(Date.from(Instant.now().minus(2, ChronoUnit.DAYS)));
        record.setVersion(TEST_ALERT_VERSION);
        record.setCategory(AlertCategory.PEOPLE);

        data = new HashMap<>();
        end = Instant.now().minus(11, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        alertData.put("NumBuyIntents", 1);
        alertData.put("NumResearchIntents", 3);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        return alerts;
    }

    private List<ActivityAlert> generateProductsAlerts() {
        List<ActivityAlert> alerts = new ArrayList<>();
        Tenant t = mainTestTenant;

        // record 1
        ActivityAlert record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY);
        record.setEntityId(TEST_ACCOUNT_ID);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(new Date());
        record.setVersion(TEST_ALERT_VERSION);
        record.setCategory(AlertCategory.PRODUCTS);

        Map<String, Object> data = new HashMap<>();
        Instant end = Instant.now();
        Instant start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        Map<String, Object> alertData = new HashMap<>();
        alertData.put("PageVisits", 392);
        alertData.put("PageName", "Main Page");
        alertData.put("ActiveContacts", 3);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 2
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY_ON_PRODUCT);
        record.setEntityId(TEST_ACCOUNT_ID);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(Date.from(Instant.now().minus(10, ChronoUnit.HOURS)));
        record.setVersion(TEST_ALERT_VERSION);
        record.setCategory(AlertCategory.PRODUCTS);

        data = new HashMap<>();
        end = Instant.now().minus(1, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        alertData.put("PageVisits", 349);
        alertData.put("PageName", "About-Us");
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 3
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.RE_ENGAGED_ACTIVITY);
        record.setEntityId(TEST_ACCOUNT_ID);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(Date.from(Instant.now().minus(1, ChronoUnit.DAYS)));
        record.setVersion(TEST_ALERT_VERSION);
        record.setCategory(AlertCategory.PRODUCTS);

        data = new HashMap<>();
        end = Instant.now().minus(7, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        alertData.put("ReEngagedContacts", 3);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);
        return alerts;
    }

    @AfterClass(groups = "deployment")
    public void cleanup() {
        testBed.deleteTenant(mainTestTenant);
    }
}
