package com.latticeengines.apps.cdl.util;

import java.util.LinkedList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.cdl.ActivityObject;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantType;

public class PriorityQueueUtilsTestNG {

    @Test(groups = "unit")
    public void testMain() {
        List<ActivityObject> activityObjectList = new LinkedList<>();
        List<ActivityObject> activityCustomer = dataProvider(TenantType.CUSTOMER);
        List<ActivityObject> activityOther = dataProvider2(TenantType.STAGING);
        activityObjectList.addAll(activityCustomer);
        activityObjectList.addAll(activityOther);
        PriorityQueueUtils.createOrUpdateQueue(activityObjectList);
        Assert.assertEquals(PriorityQueueUtils.getHighPriorityQueueSize(), 8);
        Assert.assertEquals(PriorityQueueUtils.getLowPriorityQueueSize(), 8);
        Assert.assertEquals(PriorityQueueUtils.pickFirstFromHighPriority(), "testTenant22");
        Assert.assertEquals(PriorityQueueUtils.getHighPriorityQueueSize(), 8);
        Assert.assertEquals(PriorityQueueUtils.getPositionFromHighPriorityQueue("testTenant11"), 3);
        List<String> tenantNameList = PriorityQueueUtils.getAllMemberWithSortFromHighPriorityQueue();
        Assert.assertEquals(tenantNameList.indexOf("testTenant11"), 3);
        Assert.assertEquals(PriorityQueueUtils.pickFirstFromLowPriority(), "testTenant33");
        Assert.assertEquals(PriorityQueueUtils.getLowPriorityQueueSize(), 8);
        Assert.assertEquals(PriorityQueueUtils.getPositionFromLowPriorityQueue("testTenant4"), 3);
        tenantNameList = PriorityQueueUtils.getAllMemberWithSortFromLowPriorityQueue();
        Assert.assertEquals(tenantNameList.indexOf("testTenant4"), 3);
        ActivityObject updateActivityObject = activityCustomer.get(0);
        updateActivityObject.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2018-01-01T00:00:00+0000"));
        PriorityQueueUtils.push(updateActivityObject);
        Assert.assertEquals(PriorityQueueUtils.getHighPriorityQueueSize(), 8);
        Assert.assertEquals(PriorityQueueUtils.pollFirstFromHighPriority(), "testTenant1");
        Assert.assertEquals(PriorityQueueUtils.getHighPriorityQueueSize(), 7);
        Assert.assertEquals(PriorityQueueUtils.pollFirstFromHighPriority(), "testTenant22");
        Assert.assertEquals(PriorityQueueUtils.pollFirstFromHighPriority(), "testTenant2");
        Assert.assertEquals(PriorityQueueUtils.pollFirstFromHighPriority(), "testTenant11");
        Assert.assertEquals(PriorityQueueUtils.pollFirstFromHighPriority(), "testTenant3");
        Assert.assertEquals(PriorityQueueUtils.getHighPriorityQueueSize(), 3);
        Assert.assertEquals(PriorityQueueUtils.pollFirstFromLowPriority(), "testTenant33");
        Assert.assertEquals(PriorityQueueUtils.pollFirstFromLowPriority(), "testTenant44");
        Assert.assertEquals(PriorityQueueUtils.pollFirstFromLowPriority(), "testTenant3");
        Assert.assertEquals(PriorityQueueUtils.pollFirstFromLowPriority(), "testTenant4");
        Assert.assertEquals(PriorityQueueUtils.getLowPriorityQueueSize(), 4);
        Assert.assertEquals(PriorityQueueUtils.pollFirstFromLowPriority(), "testTenant6");
        Assert.assertEquals(PriorityQueueUtils.pollFirstFromLowPriority(), "testTenant66");
    }

    private List<ActivityObject> dataProvider(TenantType type) {
        List<ActivityObject> activityObjects = new LinkedList<>();
        ActivityObject activityObject1 = new ActivityObject();
        Tenant tenant1 = new Tenant();
        tenant1.setTenantType(type);
        tenant1.setName("testTenant1");
        activityObject1.setTenant(tenant1);
        activityObject1.setScheduleNow(true);
        activityObject1.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:00:00+0000"));

        ActivityObject activityObject2 = new ActivityObject();
        Tenant tenant2 = new Tenant();
        tenant2.setTenantType(type);
        tenant2.setName("testTenant2");
        activityObject2.setTenant(tenant2);
        activityObject2.setScheduleNow(true);
        activityObject2.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-01T00:00:00+0000"));

        ActivityObject activityObject3 = new ActivityObject();
        Tenant tenant3 = new Tenant();
        tenant3.setTenantType(type);
        tenant3.setName("testTenant3");
        activityObject3.setTenant(tenant3);
        activityObject3.setScheduleNow(false);
        activityObject3.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-01-01T00:00:00+0000"));
        Action action1 = new Action();
        action1.setCreated(DateTimeUtils.convertToDateUTCISO8601("2019-01-01T00:30:00+0000"));
        action1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Action action2 = new Action();
        action2.setCreated(DateTimeUtils.convertToDateUTCISO8601("2019-01-01T09:30:00+0000"));
        action2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        List<Action> actions = new LinkedList<>();
        actions.add(action1);
        actions.add(action2);
        activityObject3.setActions(actions);

        ActivityObject activityObject4 = new ActivityObject();
        Tenant tenant4 = new Tenant();
        tenant4.setTenantType(type);
        tenant4.setName("testTenant4");
        activityObject4.setTenant(tenant4);
        activityObject4.setScheduleNow(false);
        activityObject4.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-01-01T00:00:00+0000"));
        Action action3 = new Action();
        action3.setCreated(DateTimeUtils.convertToDateUTCISO8601("2019-02-01T00:30:00+0000"));
        action3.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Action action4 = new Action();
        action4.setCreated(DateTimeUtils.convertToDateUTCISO8601("2019-02-01T09:30:00+0000"));
        action4.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        List<Action> actions1 = new LinkedList<>();
        actions1.add(action3);
        actions1.add(action4);
        activityObject4.setActions(actions1);

        ActivityObject activityObject5 = new ActivityObject();
        Tenant tenant5 = new Tenant();
        tenant5.setTenantType(type);
        tenant5.setName("testTenant5");
        activityObject5.setTenant(tenant5);
        activityObject5.setScheduleNow(false);
        activityObject5.setDataCloudRefresh(true);
        Action action5 = new Action();
        action5.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-02-01T00:30:00+0000"));
        action5.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Action action6 = new Action();
        action6.setCreated(DateTimeUtils.convertToDateUTCISO8601("2019-01-01T09:30:00+0000"));
        action6.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        List<Action> actions2 = new LinkedList<>();
        actions2.add(action5);
        actions2.add(action6);
        activityObject5.setActions(actions2);

        ActivityObject activityObject6 = new ActivityObject();
        Tenant tenant6 = new Tenant();
        tenant6.setTenantType(type);
        tenant6.setName("testTenant6");
        activityObject6.setTenant(tenant6);
        activityObject6.setScheduleNow(false);
        activityObject6.setDataCloudRefresh(true);
        Action action7 = new Action();
        action7.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-02-01T00:30:00+0000"));
        action7.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Action action8 = new Action();
        action8.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-01-01T09:30:00+0000"));
        action8.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        List<Action> actions3 = new LinkedList<>();
        actions3.add(action7);
        actions3.add(action8);
        activityObject6.setActions(actions3);

        activityObjects.add(activityObject1);
        activityObjects.add(activityObject2);
        activityObjects.add(activityObject3);
        activityObjects.add(activityObject4);
        activityObjects.add(activityObject5);
        activityObjects.add(activityObject6);
        return activityObjects;
    }

    private List<ActivityObject> dataProvider2(TenantType type) {
        List<ActivityObject> activityObjects = new LinkedList<>();
        ActivityObject activityObject1 = new ActivityObject();
        Tenant tenant1 = new Tenant();
        tenant1.setTenantType(type);
        tenant1.setName("testTenant11");
        activityObject1.setTenant(tenant1);
        activityObject1.setScheduleNow(true);
        activityObject1.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T04:00:00+0000"));

        ActivityObject activityObject2 = new ActivityObject();
        Tenant tenant2 = new Tenant();
        tenant2.setTenantType(type);
        tenant2.setName("testTenant22");
        activityObject2.setTenant(tenant2);
        activityObject2.setScheduleNow(true);
        activityObject2.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-02-01T00:00:00+0000"));

        ActivityObject activityObject3 = new ActivityObject();
        Tenant tenant3 = new Tenant();
        tenant3.setTenantType(type);
        tenant3.setName("testTenant33");
        activityObject3.setTenant(tenant3);
        activityObject3.setScheduleNow(false);
        activityObject3.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-01-01T00:00:00+0000"));
        Action action1 = new Action();
        action1.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-01-01T00:30:00+0000"));
        action1.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Action action2 = new Action();
        action2.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-01-01T09:30:00+0000"));
        action2.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        List<Action> actions  = new LinkedList<>();
        actions.add(action1);
        actions.add(action2);
        activityObject3.setActions(actions);

        ActivityObject activityObject4 = new ActivityObject();
        Tenant tenant4 = new Tenant();
        tenant4.setTenantType(type);
        tenant4.setName("testTenant44");
        activityObject4.setTenant(tenant4);
        activityObject4.setScheduleNow(false);
        activityObject4.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-01-01T00:00:00+0000"));
        Action action3 = new Action();
        action3.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-02-01T00:30:00+0000"));
        action3.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Action action4 = new Action();
        action4.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-01-01T09:30:00+0000"));
        action4.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        List<Action> actions1  = new LinkedList<>();
        actions1.add(action3);
        actions1.add(action4);
        activityObject4.setActions(actions1);

        ActivityObject activityObject5 = new ActivityObject();
        Tenant tenant5 = new Tenant();
        tenant5.setTenantType(type);
        tenant5.setName("testTenant55");
        activityObject5.setTenant(tenant5);
        activityObject5.setScheduleNow(false);
        activityObject5.setDataCloudRefresh(true);
        Action action5 = new Action();
        action5.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-02-01T01:30:00+0000"));
        action5.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Action action6 = new Action();
        action6.setCreated(DateTimeUtils.convertToDateUTCISO8601("2019-01-01T09:30:00+0000"));
        action6.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        List<Action> actions2  = new LinkedList<>();
        actions2.add(action5);
        actions2.add(action6);
        activityObject5.setActions(actions2);

        ActivityObject activityObject6 = new ActivityObject();
        Tenant tenant6 = new Tenant();
        tenant6.setTenantType(type);
        tenant6.setName("testTenant66");
        activityObject6.setTenant(tenant6);
        activityObject6.setScheduleNow(false);
        activityObject6.setDataCloudRefresh(true);
        Action action7 = new Action();
        action7.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-02-01T00:30:00+0000"));
        action7.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Action action8 = new Action();
        action8.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-01-01T10:30:00+0000"));
        action8.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        List<Action> actions3  = new LinkedList<>();
        actions3.add(action7);
        actions3.add(action8);
        activityObject6.setActions(actions3);

        ActivityObject activityObject7 = new ActivityObject();
        Tenant tenant7 = new Tenant();
        tenant7.setTenantType(type);
        tenant7.setName("testTenant77");
        activityObject7.setTenant(tenant7);
        activityObject7.setScheduleNow(false);
        Action action10 = new Action();
        action10.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-02-01T00:30:00+0000"));
        action10.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Action action9 = new Action();
        action9.setCreated(DateTimeUtils.convertToDateUTCISO8601("2018-01-01T09:30:00+0000"));
        action9.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        List<Action> actions4  = new LinkedList<>();
        actions4.add(action10);
        actions4.add(action9);
        activityObject7.setActions(actions4);

        activityObjects.add(activityObject1);
        activityObjects.add(activityObject2);
        activityObjects.add(activityObject3);
        activityObjects.add(activityObject4);
        activityObjects.add(activityObject5);
        activityObjects.add(activityObject6);
        activityObjects.add(activityObject7);
        return activityObjects;
    }
}
