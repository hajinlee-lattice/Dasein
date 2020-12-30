package com.latticeengines.app.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.transaction.Transactional;

import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.entitymanager.ActivityAlertEntityMgr;
import com.latticeengines.app.exposed.repository.datadb.ActivityAlertRepository;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants;
import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;
import com.latticeengines.domain.exposed.cdl.activitydata.ActivityAlert;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public class ActivityAlertEntityMgrImplTestNG extends AppFunctionalTestNGBase {

    @Inject
    private ActivityAlertEntityMgr activityAlertEntityMgr;

    @Inject
    private ActivityAlertRepository activityAlertRepository;

    private List<ActivityAlert> alerts;
    private Tenant t;

    private RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(10, Collections.singleton(AssertionError.class),
            null);

    @BeforeClass(groups = "functional")
    @Transactional
    public void setup() {
        t = new Tenant();
        t.setPid(-10L);
        MultiTenantContext.setTenant(t);
        alerts = generateAlerts(t);
        activityAlertRepository.saveAll(alerts);
    }

    @Test(groups = "functional")
    public void testFindByKey() {
        List<ActivityAlert> records = activityAlertEntityMgr.findTopNAlertsByEntityId("12345", BusinessEntity.Account,
                "version1", AlertCategory.PEOPLE, 6);
        assertEquals(records.size(), 2);

        records = activityAlertEntityMgr.findTopNAlertsByEntityId("23456", BusinessEntity.Account, "version1",
                AlertCategory.PRODUCTS, 6);
        assertEquals(records.size(), 4);
    }

    @Test(groups = "manual", enabled = false)
    public void testDeleteByDate() {
        int deleted = activityAlertEntityMgr
                .deleteByExpireDateBefore(Date.from(Instant.now().minus(90, ChronoUnit.DAYS)), 1);
        Assert.assertEquals(deleted, 1);
        retryTemplate.execute(ctx -> {
            List<ActivityAlert> recordsAfterDeletion = activityAlertEntityMgr.findTopNAlertsByEntityId("23456",
                    BusinessEntity.Account, "version1", AlertCategory.PRODUCTS, 6);
            assertEquals(recordsAfterDeletion.size(), 3);
            return null;
        });

        deleted = activityAlertEntityMgr.deleteByExpireDateBefore(Date.from(Instant.now().minus(90, ChronoUnit.DAYS)),
                1);
        Assert.assertEquals(deleted, 1);
        retryTemplate.execute(ctx -> {
            List<ActivityAlert> recordsAfterDeletion = activityAlertEntityMgr.findTopNAlertsByEntityId("23456",
                    BusinessEntity.Account, "version1", AlertCategory.PRODUCTS, 6);
            assertEquals(recordsAfterDeletion.size(), 2);
            return null;
        });
    }

    private List<ActivityAlert> generateAlerts(Tenant t) {
        List<ActivityAlert> alerts = new ArrayList<>();

        // record 1
        ActivityAlert record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY);
        record.setEntityId("12345");
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(new Date());
        record.setVersion("version1");
        record.setCategory(AlertCategory.PEOPLE);

        Map<String, Object> data = new HashMap<>();
        Instant end = Instant.now();
        Instant start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        Map<String, Object> alertData = new HashMap<>();
        alertData.put("PageVisits", 2);
        alertData.put("PageName", "Database Product");
        alertData.put("ActiveContacts", 3);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 2
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.ANONYMOUS_WEB_VISITS);
        record.setEntityId("12345");
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(Date.from(Instant.now().minus(10, ChronoUnit.HOURS)));
        record.setVersion("version1");
        record.setCategory(AlertCategory.PEOPLE);

        data = new HashMap<>();
        end = Instant.now().minus(1, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        alertData.put("PageVisits", 2);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 3
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.BUYING_INTENT_AROUND_PRODUCT_PAGES);
        record.setEntityId("23456");
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(Date.from(Instant.now().minus(1, ChronoUnit.DAYS)));
        record.setVersion("version1");
        record.setCategory(AlertCategory.PRODUCTS);

        data = new HashMap<>();
        end = Instant.now().minus(7, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        alertData.put("PageName", "Database Products");
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 4
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.BUYING_INTENT_AROUND_PRODUCT_PAGES);
        record.setEntityId("23456");
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(Date.from(Instant.now().minus(89, ChronoUnit.DAYS)));
        record.setVersion("version1");
        record.setCategory(AlertCategory.PRODUCTS);

        alerts.add(record);

        // record 5
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.BUYING_INTENT_AROUND_PRODUCT_PAGES);
        record.setEntityId("23456");
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(Date.from(Instant.now().minus(91, ChronoUnit.DAYS)));
        record.setVersion("version1");
        record.setCategory(AlertCategory.PRODUCTS);

        alerts.add(record);

        // record6
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.BUYING_INTENT_AROUND_PRODUCT_PAGES);
        record.setEntityId("23456");
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(t.getPid());
        record.setCreationTimestamp(Date.from(Instant.now().minus(95, ChronoUnit.DAYS)));
        record.setVersion("version1");
        record.setCategory(AlertCategory.PRODUCTS);

        alerts.add(record);

        return alerts;
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        activityAlertRepository.deleteInBatch(alerts);
        List<ActivityAlert> records = activityAlertEntityMgr.findTopNAlertsByEntityId("12345", BusinessEntity.Account,
                "version1", AlertCategory.PEOPLE, 6);
        assertEquals(records.size(), 0);
        records = activityAlertEntityMgr.findTopNAlertsByEntityId("23456", BusinessEntity.Account, "version1",
                AlertCategory.PRODUCTS, 6);
        assertEquals(records.size(), 0);
    }
}
