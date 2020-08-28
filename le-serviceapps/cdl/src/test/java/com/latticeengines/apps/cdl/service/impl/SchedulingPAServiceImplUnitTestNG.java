package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants.QUOTA_AUTO_SCHEDULE;
import static com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants.QUOTA_SCHEDULE_NOW;
import static org.mockito.Mockito.doReturn;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingResult;
import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantGroup;
import com.latticeengines.domain.exposed.security.TenantType;

public class SchedulingPAServiceImplUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(SchedulingPAServiceImplUnitTestNG.class);

    @InjectMocks
    @Spy
    private SchedulingPAServiceImpl schedulingPAService;

    private static final String SYSTEM_STATUS = "SYSTEM_STATUS";
    private static final String TENANT_ACTIVITY_LIST = "TENANT_ACTIVITY_LIST";
    private static final String TEST_SCHEDULER_NAME = "Default";
    // make sure this is outside of peace period, otherwise it's hard to test auto
    // scheduled PAs
    private static final long MOCK_CURRENT_TIME = LocalDateTime //
            .of(2020, 8, 10, 19, 0) //
            .atZone(SchedulerConstants.DEFAULT_TIMEZONE) //
            .toInstant().toEpochMilli();

    // TODO add quota & peace period specific test cases

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        schedulingPAService.setTimeClock(() -> MOCK_CURRENT_TIME);
    }

    @Test(groups = "unit")
    public void testNoRunningJobStatus() {
        Map<String, Object> map = new HashMap<>();
        map.put(SYSTEM_STATUS, getNoRunningSystemStatus());
        map.put(TENANT_ACTIVITY_LIST, getNoRetryTenantActivityList());
        doReturn(map).when(schedulingPAService).setSystemStatus(TEST_SCHEDULER_NAME);
        SchedulingResult result = schedulingPAService.getSchedulingResult(TEST_SCHEDULER_NAME, 0L);
        log.info(JsonUtils.serialize(result));
        Assert.assertEquals(result.getRetryPATenants().size(), 0);
        Assert.assertEquals(result.getNewPATenants().size(), 10);
    }

    @Test(groups = "unit")
    public void testNoRunningJobStatusWithRetry() {
        Map<String, Object> map = new HashMap<>();
        map.put(SYSTEM_STATUS, getNoRunningSystemStatus());
        map.put(TENANT_ACTIVITY_LIST, getTenantActivityListWithRetry());
        doReturn(map).when(schedulingPAService).setSystemStatus(TEST_SCHEDULER_NAME);
        SchedulingResult result = schedulingPAService.getSchedulingResult(TEST_SCHEDULER_NAME, 0L);
        log.info(JsonUtils.serialize(result));
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getRetryPATenants().size(), 2);
        Assert.assertEquals(result.getNewPATenants().size(), 8);
        // hand hold PA cannot be retried
        Assert.assertFalse(result.getRetryPATenants().contains("Tenant16"));
        //Retry invalid, last finish time < 15min, can not poll from queue
        Assert.assertFalse(result.getRetryPATenants().contains("Tenant18"));
    }

    @Test(groups = "unit")
    public void testRunningJobStatus() {
        Map<String, Object> map = new HashMap<>();
        map.put(SYSTEM_STATUS, getScheduleNowLimitSystemStatus());
        map.put(TENANT_ACTIVITY_LIST, getNoRetryTenantActivityList());
        doReturn(map).when(schedulingPAService).setSystemStatus(TEST_SCHEDULER_NAME);
        SchedulingResult result = schedulingPAService.getSchedulingResult(TEST_SCHEDULER_NAME, 0L);
        log.info(JsonUtils.serialize(result));
        Assert.assertEquals(result.getRetryPATenants().size(), 0);
        Assert.assertEquals(result.getNewPATenants().size(), 5);
    }

    @Test(groups = "unit")
    public void testRunningJobStatusWithRetry() {
        Map<String, Object> map = new HashMap<>();
        map.put(SYSTEM_STATUS, getScheduleNowLimitSystemStatus());
        map.put(TENANT_ACTIVITY_LIST, getTenantActivityListWithRetry());
        doReturn(map).when(schedulingPAService).setSystemStatus(TEST_SCHEDULER_NAME);
        SchedulingResult result = schedulingPAService.getSchedulingResult(TEST_SCHEDULER_NAME, 0L);
        log.info(JsonUtils.serialize(result));
        Assert.assertEquals(result.getRetryPATenants().size(), 2);
        Assert.assertEquals(result.getNewPATenants().size(), 3);
        //Retry invalid, last finish time < 15min, can not poll from queue
        Assert.assertFalse(result.getRetryPATenants().contains("Tenant18"));
        // hand hold PA cannot be retried
        Assert.assertFalse(result.getRetryPATenants().contains("Tenant16"));
    }

    @Test(groups = "unit")
    public void testLargeRunningJobStatus() {
        Map<String, Object> map = new HashMap<>();
        map.put(SYSTEM_STATUS, getLargeLimitSystemStatus());
        map.put(TENANT_ACTIVITY_LIST, getNoRetryTenantActivityList());
        doReturn(map).when(schedulingPAService).setSystemStatus(TEST_SCHEDULER_NAME);
        SchedulingResult result = schedulingPAService.getSchedulingResult(TEST_SCHEDULER_NAME, 0L);
        log.info(JsonUtils.serialize(result));
        Assert.assertEquals(result.getRetryPATenants().size(), 0);
        Assert.assertEquals(result.getNewPATenants().size(), 5);
        //QA tenant has limit, can not poll from queue.
        Assert.assertFalse(result.getNewPATenants().contains("tenant11"));
    }

    @Test(groups = "unit")
    public void testLargeRunningJobStatusWithRetry() {
        Map<String, Object> map = new HashMap<>();
        map.put(SYSTEM_STATUS, getLargeLimitSystemStatus());
        map.put(TENANT_ACTIVITY_LIST, getTenantActivityListWithRetry());
        doReturn(map).when(schedulingPAService).setSystemStatus(TEST_SCHEDULER_NAME);
        SchedulingResult result = schedulingPAService.getSchedulingResult(TEST_SCHEDULER_NAME, 0L);
        log.info(JsonUtils.serialize(result));
        Assert.assertEquals(result.getRetryPATenants().size(), 1);
        Assert.assertEquals(result.getNewPATenants().size(), 4);
        //Retry invalid, last finish time < 15min, can not poll from queue
        Assert.assertFalse(result.getRetryPATenants().contains("Tenant18"));
        // hand hold PA cannot be retried
        Assert.assertFalse(result.getRetryPATenants().contains("Tenant16"));
        //QA tenant has limit, can not poll from queue.
        Assert.assertFalse(result.getNewPATenants().contains("tenant2"));
    }

    @Test(groups = "unit", dataProvider = "largeTxnLimit")
    private void testLargeTxnConcurrencyLimit(int total, int scheduleNow, int large, int largeTxn,
            TenantActivity[] activities, String[] expectedTenantsToScheduleNewPA,
            String[] expectedTenantsToScheduleRetryPA) {
        SystemStatus systemStatus = newStatus(total, scheduleNow, large, largeTxn);
        systemStatus.setTenantGroups(randomGroups());
        scheduleAndVerify(systemStatus, activities, expectedTenantsToScheduleNewPA, expectedTenantsToScheduleRetryPA);
    }

    @Test(groups = "unit", dataProvider = "singleTenantGroup", singleThreaded = true)
    private void testSingleTenantGroup(TenantGroup group, String[] runningPATenantIds, TenantActivity[] activities,
            String[] expectedTenantsToScheduleNewPA, String[] expectedTenantsToScheduleRetryPA) {
        for (String tenantId : runningPATenantIds) {
            group.addTenant(CustomerSpace.parse(tenantId).toString());
        }
        // give unlimited quota to test tenant group only
        SystemStatus systemStatus = newStatus(10000, 10000, 10000, 10000);
        systemStatus.setTenantGroups(ImmutableMap.of(group.getGroupName(), group));
        scheduleAndVerify(systemStatus, activities, expectedTenantsToScheduleNewPA, expectedTenantsToScheduleRetryPA);
    }

    @DataProvider(name = "singleTenantGroup")
    private Object[][] singleTenantGroupTestData() {
        return new Object[][] { //
                /*-
                 * no current PA running
                 */

                /*-
                 * only one of t1 & t2 can run (t2 has higher priority), t3 is fine
                 */
                { //
                        new TenantGroup("g1", 1, ImmutableSet.of("t1", "t2")), //
                        new String[0], //
                        new TenantActivity[] { //
                                largeCustomerScheduleNow("t1"), //
                                largeTxnCustomerScheduleNow("t2"), //
                                largeTxnCustomerScheduleNow("t3"), //
                        }, //
                        new String[] { "t2", "t3" }, new String[0] //
                }, //
                /*-
                 * quota is 2
                 */
                { //
                        new TenantGroup("g2", 2, ImmutableSet.of("t1", "t2")), //
                        new String[0], //
                        new TenantActivity[] { //
                                largeCustomerScheduleNow("t1"), //
                                largeTxnCustomerScheduleNow("t2"), //
                                largeTxnCustomerScheduleNow("t3"), //
                        }, //
                        new String[] { "t1", "t2", "t3" }, new String[0] //
                }, //
                /*-
                 * quota > no. tenant in queue
                 */
                { //
                        new TenantGroup("g3", 5, ImmutableSet.of("t1", "t2")), //
                        new String[0], //
                        new TenantActivity[] { //
                                largeCustomerScheduleNow("t1"), //
                                largeTxnCustomerScheduleNow("t2"), //
                                largeTxnCustomerScheduleNow("t3"), //
                        }, //
                        new String[] { "t1", "t2", "t3" }, new String[0] //
                }, //

                /*-
                 * some PA is currently running
                 */

                /*-
                 * quota=1, already one tenant running
                 */
                { //
                        new TenantGroup("g11", 1, ImmutableSet.of("t1", "t2")), //
                        new String[] { "t1" }, //
                        new TenantActivity[] { //
                                largeTxnCustomerScheduleNow("t2"), //
                                largeTxnCustomerScheduleNow("t3"), //
                        }, //
                        new String[] { "t3" }, new String[0] //
                }, //
                /*-
                 * make sure quota also apply to retry
                 */
                { //
                        new TenantGroup("g11", 1, ImmutableSet.of("t1", "t2")), //
                        new String[] { "t1" }, //
                        new TenantActivity[] { //
                                retry("t2"), //
                                largeTxnCustomerScheduleNow("t3"), //
                        }, //
                        new String[] { "t3" }, new String[0] //
                }, //

                /*-
                 * quota=1, running tenant is not in group, still can schedule
                 */
                { //
                        new TenantGroup("g12", 1, ImmutableSet.of("t1", "t2")), //
                        new String[] { "t3" }, //
                        new TenantActivity[] { //
                                largeTxnCustomerScheduleNow("t2"), //
                                largeTxnCustomerScheduleNow("t4"), //
                        }, //
                        new String[] { "t2", "t4" }, new String[0] //
                }, //

                /*-
                 * quota=2, only one in group running, can schedule
                 */
                { //
                        new TenantGroup("g13", 2, ImmutableSet.of("t1", "t2")), //
                        new String[] { "t1", "t3" }, //
                        new TenantActivity[] { //
                                retry("t2"), //
                                largeTxnAutoSchedule("t4", 1L), //
                        }, //
                        new String[] { "t4" }, new String[] { "t2" } //
                }, //

                /*-
                 * quota=2, one in group already running, only one of two in queue can be scheduled.
                 * retry one has priority
                 */
                { //
                        new TenantGroup("g14", 2, ImmutableSet.of("t1", "t2", "t3")), //
                        new String[] { "t1" }, //
                        new TenantActivity[] { //
                                retry("t2"), //
                                customerScheduleNow("t3"), //
                                largeTxnAutoSchedule("t4", 1L), //
                        }, //
                        new String[] { "t4" }, new String[] { "t2" } //
                }, //

                /*-
                 * quota=2, two from the same queue fighting for the spot
                 */
                { //
                        new TenantGroup("g15", 2, ImmutableSet.of("t1", "t2", "t3")), //
                        new String[] { "t1", "t4" }, //
                        new TenantActivity[] { //
                                largeTxnAutoSchedule("t2", 1L), //
                                largeTxnAutoSchedule("t3", 2L), //
                        }, //
                        new String[] { "t2" }, new String[0] //
                }, //
        }; //

    }

    @DataProvider(name = "largeTxnLimit")
    private Object[][] largeTxnConcurrencyLimitNewPATestData() {
        return new Object[][] { //
                /*-
                 * one large txn & one large tenant, both can schedule since they don't share quota anymore
                 */
                { //
                        2, 2, 1, 1, //
                        new TenantActivity[] { //
                                largeCustomerScheduleNow("t1"), //
                                largeTxnCustomerScheduleNow("t2"), //
                        }, //
                        new String[] { "t1", "t2" }, new String[0] //
                }, //
                /*-
                 * a lot of normal large tenant, still should schedule for large txn tenant
                 */
                { //
                        1, 1, 0, 1, //
                        new TenantActivity[] { //
                                largeCustomerScheduleNow("t1"), //
                                largeCustomerScheduleNow("t2"), //
                                largeCustomerScheduleNow("t3"), //
                                largeCustomerScheduleNow("t4"), //
                                largeTxnCustomerScheduleNow("t5"), //
                                largeTxnAutoSchedule("t6", 1L), //
                                customerScheduleNow("t7"), //
                                customerScheduleNow("t8"), //
                                customerScheduleNow("t9"), //
                                customerScheduleNow("t10"), //
                        }, //
                        new String[] { "t5" }, new String[0] //
                }, //
                /*-
                 * only two large txn tenant from schedule now queue can run
                 * normal tenant and non-txn large tenant still can run
                 */
                { //
                        10, 10, 10, 2, //
                        new TenantActivity[] { //
                                largeTxnCustomerScheduleNow("t1"), //
                                largeTxnCustomerScheduleNow("t3"), //
                                largeTxnAutoSchedule("t2", 1L), //
                                customerScheduleNow("t4"), //
                                largeCustomerScheduleNow("t5"), }, //
                        new String[] { "t1", "t3", "t4", "t5" }, new String[0] //
                }, //
                /*-
                 * all 3 can run
                 */
                { //
                        10, 10, 10, 3, //
                        new TenantActivity[] { //
                                largeTxnCustomerScheduleNow("t1"), //
                                largeTxnCustomerScheduleNow("t3"), //
                                largeTxnAutoSchedule("t2", 1L) //
                        }, //
                        new String[] { "t1", "t2", "t3" }, new String[0] //
                }, //
                /*-
                 * txn quota = 1, only the one with earlier first action time will be scheduled
                 */
                { //
                        10, 10, 10, 1, //
                        new TenantActivity[] { //
                                largeTxnAutoSchedule("t1", 1L), //
                                largeTxnAutoSchedule("t2", 5L) //
                        }, //
                        new String[] { "t1" }, new String[0] //
                }, //
                /*-
                 * have enough quota
                 */
                { //
                        10, 10, 10, 3, //
                        new TenantActivity[] { //
                                largeTxnAutoSchedule("t1", 1L), //
                                largeTxnAutoSchedule("t2", 5L), //
                                largeTxnAutoSchedule("t3", 10L) //
                        }, //
                        new String[] { "t1", "t2", "t3" }, new String[0] //
                }, //
                { //
                        10, 10, 10, 10, //
                        new TenantActivity[] { //
                                largeCustomerScheduleNow("t1"), //
                                largeTxnCustomerScheduleNow("t2"), //
                                largeTxnAutoSchedule("t3", 10L), //
                                largeTxnAutoSchedule("t4", 15L) //
                        }, //
                        new String[] { "t1", "t2", "t3", "t4" }, new String[0] //
                }, //

                /*-
                 * tenants with retries should also be limited to the same quota
                 */
                { //
                        10, 10, 1, 1, //
                        new TenantActivity[] { //
                                largeTxnRetry("t1"), //
                                largeRetry("t2"), //
                                largeTxnAutoSchedule("t3", 10L), //
                                largeTxnAutoSchedule("t4", 15L), //
                                largeCustomerScheduleNow("t5"), //
                                customerScheduleNow("t6"), //
                        }, //
                        new String[] { "t6" }, new String[] { "t1" } //
                }, //
                { //
                        10, 10, 2, 1, //
                        new TenantActivity[] { //
                                largeTxnRetry("t1"), //
                                largeTxnRetry("t2"), //
                                largeTxnAutoSchedule("t3", 10L), //
                                largeTxnAutoSchedule("t4", 15L), //
                                largeCustomerScheduleNow("t5"), //
                                customerScheduleNow("t6"), //
                        }, //
                        new String[] { "t5", "t6" }, new String[] { "t1" } //
                }, //
        }; //
    }

    private void scheduleAndVerify(SystemStatus systemStatus, TenantActivity[] activities,
            String[] expectedTenantsToScheduleNewPA, String[] expectedTenantsToScheduleRetryPA) {
        Map<String, Object> map = new HashMap<>();
        map.put(SYSTEM_STATUS, systemStatus);
        map.put(TENANT_ACTIVITY_LIST, Arrays.asList(activities));
        doReturn(map).when(schedulingPAService).setSystemStatus(TEST_SCHEDULER_NAME);
        SchedulingResult result = schedulingPAService.getSchedulingResult(TEST_SCHEDULER_NAME, 0L);
        Assert.assertEquals(result.getRetryPATenants().size(), expectedTenantsToScheduleRetryPA.length);
        Assert.assertEquals(result.getRetryPATenants(), new HashSet<>(Arrays //
                .stream(expectedTenantsToScheduleRetryPA) //
                .map(tenantId -> CustomerSpace.parse(tenantId).toString()) //
                .collect(Collectors.toList())));
        Assert.assertEquals(result.getNewPATenants().size(), expectedTenantsToScheduleNewPA.length);
        Assert.assertEquals(result.getNewPATenants(), new HashSet<>(Arrays //
                .stream(expectedTenantsToScheduleNewPA) //
                .map(tenantId -> CustomerSpace.parse(tenantId).toString()) //
                .collect(Collectors.toList())));
    }

    // add a random group that doesn't contain any existing test tenant name
    // just to make sure this new constraint won't break any existing functionality
    private Map<String, TenantGroup> randomGroups() {
        List<TenantGroup> groups = Arrays.asList( //
                new TenantGroup("g1", 2, ImmutableSet.of("t99999", "t99998", "t99997")),
                new TenantGroup("g2", 1, ImmutableSet.of("t99996", "t99995")));
        return groups.stream().collect(Collectors.toMap(TenantGroup::getGroupName, g -> g));
    }

    private SystemStatus getNoRunningSystemStatus() {
        SystemStatus systemStatus = new SystemStatus();
        systemStatus.setCanRunJobCount(10);
        systemStatus.setCanRunLargeJobCount(2);
        systemStatus.setCanRunScheduleNowJobCount(5);
        systemStatus.setRunningLargeJobCount(0);
        Set<String> runningPATenantId = new HashSet<>();
        systemStatus.setRunningPATenantId(runningPATenantId);
        systemStatus.setRunningTotalCount(0);
        systemStatus.setRunningScheduleNowCount(0);
        Set<String> largeJobTenantId = new HashSet<>();
        largeJobTenantId.add("Tenant3");
        largeJobTenantId.add("Tenant4");
        largeJobTenantId.add("Tenant5");
        largeJobTenantId.add("Tenant6");
        largeJobTenantId.add("Tenant7");
        largeJobTenantId.add("Tenant8");
        largeJobTenantId.add("Tenant17");
        systemStatus.setTenantGroups(randomGroups());
        systemStatus.setLargeJobTenantId(largeJobTenantId);
        return systemStatus;
    }

    private SystemStatus getScheduleNowLimitSystemStatus() {
        SystemStatus systemStatus = new SystemStatus();
        systemStatus.setCanRunJobCount(5);
        systemStatus.setCanRunLargeJobCount(2);
        systemStatus.setCanRunScheduleNowJobCount(0);
        systemStatus.setRunningLargeJobCount(0);
        systemStatus.setRunningPATenantId(new HashSet<>());
        systemStatus.setRunningTotalCount(5);
        systemStatus.setRunningScheduleNowCount(5);
        Set<String> largeJobTenantId = new HashSet<>();
        largeJobTenantId.add("Tenant3");
        largeJobTenantId.add("Tenant4");
        largeJobTenantId.add("Tenant5");
        largeJobTenantId.add("Tenant6");
        largeJobTenantId.add("Tenant7");
        largeJobTenantId.add("Tenant8");
        largeJobTenantId.add("Tenant17");
        systemStatus.setTenantGroups(randomGroups());
        systemStatus.setLargeJobTenantId(largeJobTenantId);
        return systemStatus;
    }

    private SystemStatus getLargeLimitSystemStatus() {
        SystemStatus systemStatus = new SystemStatus();
        systemStatus.setCanRunJobCount(5);
        systemStatus.setCanRunLargeJobCount(0);
        systemStatus.setCanRunScheduleNowJobCount(3);
        systemStatus.setRunningLargeJobCount(2);
        systemStatus.setRunningPATenantId(new HashSet<>());
        systemStatus.setRunningTotalCount(4);
        systemStatus.setRunningScheduleNowCount(2);
        Set<String> largeJobTenantId = new HashSet<>();
        largeJobTenantId.add("Tenant3");
        largeJobTenantId.add("Tenant4");
        largeJobTenantId.add("Tenant5");
        largeJobTenantId.add("Tenant6");
        largeJobTenantId.add("Tenant7");
        largeJobTenantId.add("Tenant8");
        largeJobTenantId.add("Tenant17");
        systemStatus.setTenantGroups(randomGroups());
        systemStatus.setLargeJobTenantId(largeJobTenantId);
        return systemStatus;
    }

    private List<TenantActivity> getNoRetryTenantActivityList() {
        List<TenantActivity> tenantActivityList = new LinkedList<>();

        TenantActivity tenantActivity1 = new TenantActivity();
        tenantActivity1.setRetry(false);
        tenantActivity1.setDataCloudRefresh(false);
        tenantActivity1.setScheduledNow(true);
        tenantActivity1.setTenantType(TenantType.CUSTOMER);
        tenantActivity1.setTenantId("Tenant1");
        tenantActivity1.setLarge(false);
        tenantActivity1.setAutoSchedule(false);
        tenantActivity1.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity1);

        TenantActivity tenantActivity2 = new TenantActivity();
        tenantActivity2.setRetry(false);
        tenantActivity2.setDataCloudRefresh(false);
        tenantActivity2.setScheduledNow(true);
        tenantActivity2.setTenantType(TenantType.QA);
        tenantActivity2.setTenantId("Tenant2");
        tenantActivity2.setLarge(false);
        tenantActivity2.setAutoSchedule(true);
        tenantActivity2.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:05:00+0000").getTime());
        tenantActivity2.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-01T00:00:00+0000").getTime());
        tenantActivity2.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-02T00:00:00+0000").getTime());
        tenantActivity2.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity2);

        TenantActivity tenantActivity3 = new TenantActivity();
        tenantActivity3.setRetry(false);
        tenantActivity3.setDataCloudRefresh(false);
        tenantActivity3.setScheduledNow(true);
        tenantActivity3.setTenantType(TenantType.QA);
        tenantActivity3.setTenantId("Tenant3");
        tenantActivity3.setLarge(true);
        tenantActivity3.setAutoSchedule(false);
        tenantActivity3.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:01:00+0000").getTime());
        tenantActivityList.add(tenantActivity3);

        TenantActivity tenantActivity4 = new TenantActivity();
        tenantActivity4.setRetry(false);
        tenantActivity4.setDataCloudRefresh(false);
        tenantActivity4.setScheduledNow(true);
        tenantActivity4.setTenantType(TenantType.CUSTOMER);
        tenantActivity4.setTenantId("Tenant4");
        tenantActivity4.setLarge(true);
        tenantActivity4.setAutoSchedule(false);
        tenantActivity4.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:01:30+0000").getTime());
        tenantActivityList.add(tenantActivity4);

        TenantActivity tenantActivity5 = new TenantActivity();
        tenantActivity5.setRetry(false);
        tenantActivity5.setDataCloudRefresh(false);
        tenantActivity5.setScheduledNow(true);
        tenantActivity5.setTenantType(TenantType.CUSTOMER);
        tenantActivity5.setTenantId("Tenant5");
        tenantActivity5.setLarge(true);
        tenantActivity5.setAutoSchedule(false);
        tenantActivity5.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:02:30+0000").getTime());
        tenantActivityList.add(tenantActivity5);

        TenantActivity tenantActivity6 = new TenantActivity();
        tenantActivity6.setRetry(false);
        tenantActivity6.setDataCloudRefresh(false);
        tenantActivity6.setScheduledNow(true);
        tenantActivity6.setTenantType(TenantType.CUSTOMER);
        tenantActivity6.setTenantId("Tenant6");
        tenantActivity6.setLarge(true);
        tenantActivity6.setAutoSchedule(true);
        tenantActivity6.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:05:00+0000").getTime());
        tenantActivity6.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-01T00:00:00+0000").getTime());
        tenantActivity6.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-02T00:00:00+0000").getTime());
        tenantActivity6.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:02:30+0000").getTime());
        tenantActivityList.add(tenantActivity6);

        TenantActivity tenantActivity7 = new TenantActivity();
        tenantActivity7.setRetry(false);
        tenantActivity7.setDataCloudRefresh(false);
        tenantActivity7.setScheduledNow(true);
        tenantActivity7.setTenantType(TenantType.CUSTOMER);
        tenantActivity7.setTenantId("Tenant7");
        tenantActivity7.setLarge(true);
        tenantActivity7.setAutoSchedule(true);
        tenantActivity7.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:05:00+0000").getTime());
        tenantActivity7.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-01T01:00:00+0000").getTime());
        tenantActivity7.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-02T01:00:00+0000").getTime());
        tenantActivity7.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:02:30+0000").getTime());
        tenantActivityList.add(tenantActivity7);

        TenantActivity tenantActivity8 = new TenantActivity();
        tenantActivity8.setRetry(false);
        tenantActivity8.setDataCloudRefresh(false);
        tenantActivity8.setScheduledNow(false);
        tenantActivity8.setTenantType(TenantType.CUSTOMER);
        tenantActivity8.setTenantId("Tenant8");
        tenantActivity8.setLarge(true);
        tenantActivity8.setAutoSchedule(true);
        tenantActivity8.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T08:05:00+0000").getTime());
        tenantActivity8.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T01:00:00+0000").getTime());
        tenantActivity8.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-02T01:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity8);

        TenantActivity tenantActivity9 = new TenantActivity();
        tenantActivity9.setRetry(false);
        tenantActivity9.setDataCloudRefresh(false);
        tenantActivity9.setScheduledNow(false);
        tenantActivity9.setTenantType(TenantType.CUSTOMER);
        tenantActivity9.setTenantId("Tenant9");
        tenantActivity9.setLarge(false);
        tenantActivity9.setAutoSchedule(true);
        tenantActivity9.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T08:05:00+0000").getTime());
        tenantActivity9.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T12:00:00+0000").getTime());
        tenantActivity9.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-02T01:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity9);

        TenantActivity tenantActivity10 = new TenantActivity();
        tenantActivity10.setRetry(false);
        tenantActivity10.setDataCloudRefresh(false);
        tenantActivity10.setScheduledNow(false);
        tenantActivity10.setTenantType(TenantType.QA);
        tenantActivity10.setTenantId("Tenant10");
        tenantActivity10.setLarge(false);
        tenantActivity10.setAutoSchedule(true);
        tenantActivity10.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T08:05:00+0000").getTime());
        tenantActivity10.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T01:00:00+0000").getTime());
        tenantActivity10.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-02T01:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity10);

        TenantActivity tenantActivity11 = new TenantActivity();
        tenantActivity11.setRetry(false);
        tenantActivity11.setDataCloudRefresh(true);
        tenantActivity11.setScheduledNow(false);
        tenantActivity11.setTenantType(TenantType.QA);
        tenantActivity11.setTenantId("Tenant11");
        tenantActivity11.setLarge(false);
        tenantActivity11.setAutoSchedule(true);
        tenantActivity11.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T08:05:00+0000").getTime());
        tenantActivity11.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-02T01:00:00+0000").getTime());
        tenantActivity11.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-02T01:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity11);

        TenantActivity tenantActivity12 = new TenantActivity();
        tenantActivity12.setRetry(false);
        tenantActivity12.setDataCloudRefresh(true);
        tenantActivity12.setScheduledNow(false);
        tenantActivity12.setTenantType(TenantType.QA);
        tenantActivity12.setTenantId("Tenant12");
        tenantActivity12.setLarge(false);
        tenantActivity12.setAutoSchedule(false);
        tenantActivityList.add(tenantActivity12);

        TenantActivity tenantActivity13 = new TenantActivity();
        tenantActivity13.setRetry(false);
        tenantActivity13.setDataCloudRefresh(true);
        tenantActivity13.setScheduledNow(true);
        tenantActivity13.setTenantType(TenantType.QA);
        tenantActivity13.setTenantId("Tenant13");
        tenantActivity13.setLarge(false);
        tenantActivity13.setAutoSchedule(false);
        tenantActivity13.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T08:05:00+0000").getTime());
        tenantActivityList.add(tenantActivity13);

        TenantActivity tenantActivity14 = new TenantActivity();
        tenantActivity14.setRetry(false);
        tenantActivity14.setDataCloudRefresh(true);
        tenantActivity14.setScheduledNow(false);
        tenantActivity14.setTenantType(TenantType.QA);
        tenantActivity14.setTenantId("Tenant14");
        tenantActivity14.setLarge(false);
        tenantActivity14.setAutoSchedule(false);
        tenantActivityList.add(tenantActivity14);
        addQuota(tenantActivityList);

        return tenantActivityList;
    }

    private List<TenantActivity> getTenantActivityListWithRetry() {
        List<TenantActivity> tenantActivityList = new LinkedList<>();

        TenantActivity tenantActivity1 = new TenantActivity();
        tenantActivity1.setRetry(false);
        tenantActivity1.setDataCloudRefresh(false);
        tenantActivity1.setScheduledNow(true);
        tenantActivity1.setTenantType(TenantType.CUSTOMER);
        tenantActivity1.setTenantId("Tenant1");
        tenantActivity1.setLarge(false);
        tenantActivity1.setAutoSchedule(false);
        tenantActivity1.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity1);

        TenantActivity tenantActivity2 = new TenantActivity();
        tenantActivity2.setRetry(false);
        tenantActivity2.setDataCloudRefresh(false);
        tenantActivity2.setScheduledNow(true);
        tenantActivity2.setTenantType(TenantType.QA);
        tenantActivity2.setTenantId("Tenant2");
        tenantActivity2.setLarge(false);
        tenantActivity2.setAutoSchedule(true);
        tenantActivity2.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:05:00+0000").getTime());
        tenantActivity2.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-01T00:00:00+0000").getTime());
        tenantActivity2.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-02T00:00:00+0000").getTime());
        tenantActivity2.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity2);

        TenantActivity tenantActivity3 = new TenantActivity();
        tenantActivity3.setRetry(false);
        tenantActivity3.setDataCloudRefresh(false);
        tenantActivity3.setScheduledNow(true);
        tenantActivity3.setTenantType(TenantType.QA);
        tenantActivity3.setTenantId("Tenant3");
        tenantActivity3.setLarge(true);
        tenantActivity3.setAutoSchedule(false);
        tenantActivity3.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:01:00+0000").getTime());
        tenantActivityList.add(tenantActivity3);

        TenantActivity tenantActivity4 = new TenantActivity();
        tenantActivity4.setRetry(false);
        tenantActivity4.setDataCloudRefresh(false);
        tenantActivity4.setScheduledNow(true);
        tenantActivity4.setTenantType(TenantType.CUSTOMER);
        tenantActivity4.setTenantId("Tenant4");
        tenantActivity4.setLarge(true);
        tenantActivity4.setAutoSchedule(false);
        tenantActivity4.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:01:30+0000").getTime());
        tenantActivityList.add(tenantActivity4);

        TenantActivity tenantActivity5 = new TenantActivity();
        tenantActivity5.setRetry(false);
        tenantActivity5.setDataCloudRefresh(false);
        tenantActivity5.setScheduledNow(true);
        tenantActivity5.setTenantType(TenantType.CUSTOMER);
        tenantActivity5.setTenantId("Tenant5");
        tenantActivity5.setLarge(true);
        tenantActivity5.setAutoSchedule(false);
        tenantActivity5.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:02:30+0000").getTime());
        tenantActivityList.add(tenantActivity5);

        TenantActivity tenantActivity6 = new TenantActivity();
        tenantActivity6.setRetry(false);
        tenantActivity6.setDataCloudRefresh(false);
        tenantActivity6.setScheduledNow(true);
        tenantActivity6.setTenantType(TenantType.CUSTOMER);
        tenantActivity6.setTenantId("Tenant6");
        tenantActivity6.setLarge(true);
        tenantActivity6.setAutoSchedule(true);
        tenantActivity6.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:05:00+0000").getTime());
        tenantActivity6.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-01T00:00:00+0000").getTime());
        tenantActivity6.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-02T00:00:00+0000").getTime());
        tenantActivity6.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:02:30+0000").getTime());
        tenantActivityList.add(tenantActivity6);

        TenantActivity tenantActivity7 = new TenantActivity();
        tenantActivity7.setRetry(false);
        tenantActivity7.setDataCloudRefresh(false);
        tenantActivity7.setScheduledNow(true);
        tenantActivity7.setTenantType(TenantType.CUSTOMER);
        tenantActivity7.setTenantId("Tenant7");
        tenantActivity7.setLarge(true);
        tenantActivity7.setAutoSchedule(true);
        tenantActivity7.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:05:00+0000").getTime());
        tenantActivity7.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-01T01:00:00+0000").getTime());
        tenantActivity7.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-03-02T01:00:00+0000").getTime());
        tenantActivity7.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T00:02:30+0000").getTime());
        tenantActivityList.add(tenantActivity7);

        TenantActivity tenantActivity8 = new TenantActivity();
        tenantActivity8.setRetry(false);
        tenantActivity8.setDataCloudRefresh(false);
        tenantActivity8.setScheduledNow(false);
        tenantActivity8.setTenantType(TenantType.CUSTOMER);
        tenantActivity8.setTenantId("Tenant8");
        tenantActivity8.setLarge(true);
        tenantActivity8.setAutoSchedule(true);
        tenantActivity8.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T08:05:00+0000").getTime());
        tenantActivity8.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T01:00:00+0000").getTime());
        tenantActivity8.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-02T01:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity8);

        TenantActivity tenantActivity9 = new TenantActivity();
        tenantActivity9.setRetry(false);
        tenantActivity9.setDataCloudRefresh(false);
        tenantActivity9.setScheduledNow(false);
        tenantActivity9.setTenantType(TenantType.CUSTOMER);
        tenantActivity9.setTenantId("Tenant9");
        tenantActivity9.setLarge(false);
        tenantActivity9.setAutoSchedule(true);
        tenantActivity9.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T08:05:00+0000").getTime());
        tenantActivity9.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T12:00:00+0000").getTime());
        tenantActivity9.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-02T01:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity9);

        TenantActivity tenantActivity10 = new TenantActivity();
        tenantActivity10.setRetry(false);
        tenantActivity10.setDataCloudRefresh(false);
        tenantActivity10.setScheduledNow(false);
        tenantActivity10.setTenantType(TenantType.QA);
        tenantActivity10.setTenantId("Tenant10");
        tenantActivity10.setLarge(false);
        tenantActivity10.setAutoSchedule(true);
        tenantActivity10.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T08:05:00+0000").getTime());
        tenantActivity10.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T01:00:00+0000").getTime());
        tenantActivity10.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-02T01:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity10);

        TenantActivity tenantActivity11 = new TenantActivity();
        tenantActivity11.setRetry(false);
        tenantActivity11.setDataCloudRefresh(true);
        tenantActivity11.setScheduledNow(false);
        tenantActivity11.setTenantType(TenantType.QA);
        tenantActivity11.setTenantId("Tenant11");
        tenantActivity11.setLarge(false);
        tenantActivity11.setAutoSchedule(true);
        tenantActivity11.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T08:05:00+0000").getTime());
        tenantActivity11.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-02T01:00:00+0000").getTime());
        tenantActivity11.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-02T01:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity11);

        TenantActivity tenantActivity12 = new TenantActivity();
        tenantActivity12.setRetry(false);
        tenantActivity12.setDataCloudRefresh(true);
        tenantActivity12.setScheduledNow(false);
        tenantActivity12.setTenantType(TenantType.QA);
        tenantActivity12.setTenantId("Tenant12");
        tenantActivity12.setLarge(false);
        tenantActivity12.setAutoSchedule(false);
        tenantActivityList.add(tenantActivity12);

        TenantActivity tenantActivity13 = new TenantActivity();
        tenantActivity13.setRetry(false);
        tenantActivity13.setDataCloudRefresh(true);
        tenantActivity13.setScheduledNow(true);
        tenantActivity13.setTenantType(TenantType.CUSTOMER);
        tenantActivity13.setTenantId("Tenant13");
        tenantActivity13.setLarge(false);
        tenantActivity13.setAutoSchedule(false);
        tenantActivity13.setScheduleTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T08:05:00+0000").getTime());
        tenantActivityList.add(tenantActivity13);

        TenantActivity tenantActivity14 = new TenantActivity();
        tenantActivity14.setRetry(false);
        tenantActivity14.setDataCloudRefresh(true);
        tenantActivity14.setScheduledNow(false);
        tenantActivity14.setTenantType(TenantType.CUSTOMER);
        tenantActivity14.setTenantId("Tenant14");
        tenantActivity14.setLarge(false);
        tenantActivity14.setAutoSchedule(false);
        tenantActivityList.add(tenantActivity14);

        TenantActivity tenantActivity15 = new TenantActivity();
        tenantActivity15.setRetry(true);
        tenantActivity15.setLastFinishTime(MOCK_CURRENT_TIME - 1000000);
        tenantActivity15.setDataCloudRefresh(true);
        tenantActivity15.setScheduledNow(false);
        tenantActivity15.setTenantType(TenantType.CUSTOMER);
        tenantActivity15.setTenantId("Tenant15");
        tenantActivity15.setLarge(false);
        tenantActivity15.setAutoSchedule(false);
        tenantActivityList.add(tenantActivity15);

        TenantActivity tenantActivity16 = new TenantActivity();
        tenantActivity16.setRetry(true);
        tenantActivity16.setHandHoldTenant(true);
        tenantActivity16.setLastFinishTime(MOCK_CURRENT_TIME - 9000000);
        tenantActivity16.setDataCloudRefresh(false);
        tenantActivity16.setScheduledNow(false);
        tenantActivity16.setTenantType(TenantType.CUSTOMER);
        tenantActivity16.setTenantId("Tenant16");
        tenantActivity16.setLarge(false);
        tenantActivity16.setAutoSchedule(false);
        tenantActivityList.add(tenantActivity16);

        TenantActivity tenantActivity17 = new TenantActivity();
        tenantActivity17.setRetry(true);
        tenantActivity17.setLastFinishTime(MOCK_CURRENT_TIME - 9000000);
        tenantActivity17.setDataCloudRefresh(false);
        tenantActivity17.setScheduledNow(false);
        tenantActivity17.setTenantType(TenantType.CUSTOMER);
        tenantActivity17.setTenantId("Tenant17");
        tenantActivity17.setLarge(true);
        tenantActivity17.setAutoSchedule(false);
        tenantActivityList.add(tenantActivity17);

        TenantActivity tenantActivity18 = new TenantActivity();
        tenantActivity18.setRetry(true);
        tenantActivity18.setLastFinishTime(MOCK_CURRENT_TIME);
        tenantActivity18.setDataCloudRefresh(false);
        tenantActivity18.setScheduledNow(false);
        tenantActivity18.setTenantType(TenantType.CUSTOMER);
        tenantActivity18.setTenantId("Tenant18");
        tenantActivity18.setLarge(false);
        tenantActivity18.setAutoSchedule(false);
        tenantActivityList.add(tenantActivity18);

        TenantActivity tenantActivity19 = new TenantActivity();
        tenantActivity19.setRetry(true);
        // null last finished time
        tenantActivity19.setDataCloudRefresh(false);
        tenantActivity19.setScheduledNow(false);
        tenantActivity19.setTenantType(TenantType.QA);
        tenantActivity19.setTenantId("Tenant19");
        tenantActivity19.setLarge(false);
        tenantActivity19.setAutoSchedule(true);
        tenantActivity19.setInvokeTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T08:05:00+0000").getTime());
        tenantActivity19.setFirstActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-01T01:00:00+0000").getTime());
        tenantActivity19.setLastActionTime(DateTimeUtils.convertToDateUTCISO8601("2019-04-02T01:00:00+0000").getTime());
        tenantActivityList.add(tenantActivity19);
        addQuota(tenantActivityList);

        return tenantActivityList;
    }

    private void addQuota(List<TenantActivity> tenants) {
        tenants.forEach(
                tenant -> tenant.setNotExceededQuotaNames(ImmutableSet.of(QUOTA_AUTO_SCHEDULE, QUOTA_SCHEDULE_NOW)));
    }

    private SystemStatus newStatus(int totalLimit, int scheduleNowLimit, int largeLimit, int largeTxnLimit) {
        SystemStatus status = new SystemStatus();
        status.setCanRunJobCount(totalLimit);
        status.setCanRunLargeJobCount(largeLimit);
        status.setCanRunScheduleNowJobCount(scheduleNowLimit);
        status.setCanRunLargeTxnJobCount(largeTxnLimit);
        return status;
    }

    private TenantActivity retry(String tenantId) {
        return TenantActivity.Builder.newInstance() //
                .withTenantId(CustomerSpace.parse(tenantId).toString()) //
                .withTenantType(TenantType.CUSTOMER) //
                .withLastFinishTime(1L) //
                .withNotExceededQuotaNames(ImmutableSet.of(QUOTA_AUTO_SCHEDULE, QUOTA_SCHEDULE_NOW)) //
                .retry() //
                .build();
    }

    private TenantActivity largeRetry(String tenantId) {
        return TenantActivity.Builder.newInstance() //
                .withTenantId(CustomerSpace.parse(tenantId).toString()) //
                .withTenantType(TenantType.CUSTOMER) //
                .withLastFinishTime(1L) //
                .withNotExceededQuotaNames(ImmutableSet.of(QUOTA_AUTO_SCHEDULE, QUOTA_SCHEDULE_NOW)) //
                .large() //
                .retry() //
                .build();
    }

    private TenantActivity largeTxnRetry(String tenantId) {
        return TenantActivity.Builder.newInstance() //
                .withTenantId(CustomerSpace.parse(tenantId).toString()) //
                .withTenantType(TenantType.CUSTOMER) //
                .withLastFinishTime(1L) //
                .withNotExceededQuotaNames(ImmutableSet.of(QUOTA_AUTO_SCHEDULE, QUOTA_SCHEDULE_NOW)) //
                .large() //
                .largeTxn() //
                .retry() //
                .build();
    }

    // ordinary schedule now tenant
    private TenantActivity customerScheduleNow(String tenantId) {
        return TenantActivity.Builder.newInstance() //
                .withTenantId(CustomerSpace.parse(tenantId).toString()) //
                .withScheduledNow(true) //
                .withScheduleTime(MOCK_CURRENT_TIME) //
                .withTenantType(TenantType.CUSTOMER) //
                .withNotExceededQuotaNames(ImmutableSet.of(QUOTA_AUTO_SCHEDULE, QUOTA_SCHEDULE_NOW)) //
                .build();
    }

    private TenantActivity largeCustomerScheduleNow(String tenantId) {
        return TenantActivity.Builder.newInstance() //
                .withTenantId(CustomerSpace.parse(tenantId).toString()) //
                .withScheduledNow(true) //
                .withScheduleTime(MOCK_CURRENT_TIME) //
                .withNotExceededQuotaNames(ImmutableSet.of(QUOTA_AUTO_SCHEDULE, QUOTA_SCHEDULE_NOW)) //
                .large() //
                .withTenantType(TenantType.CUSTOMER) //
                .build();
    }

    private TenantActivity largeTxnCustomerScheduleNow(String tenantId) {
        return TenantActivity.Builder.newInstance() //
                .withTenantId(CustomerSpace.parse(tenantId).toString()) //
                .withScheduledNow(true) //
                .withScheduleTime(MOCK_CURRENT_TIME) //
                .withNotExceededQuotaNames(ImmutableSet.of(QUOTA_AUTO_SCHEDULE, QUOTA_SCHEDULE_NOW)) //
                .largeTxn() //
                .withTenantType(TenantType.CUSTOMER) //
                .build();
    }

    private TenantActivity largeTxnAutoSchedule(String tenantId, long firstActionTime) {
        return TenantActivity.Builder.newInstance() //
                .withTenantId(CustomerSpace.parse(tenantId).toString()) //
                .autoSchedule() //
                .withInvokeTime(MOCK_CURRENT_TIME) //
                .withFirstActionTime(firstActionTime) //
                .withLastActionTime(MOCK_CURRENT_TIME - 86400 * 1000) //
                .withNotExceededQuotaNames(ImmutableSet.of(QUOTA_AUTO_SCHEDULE, QUOTA_SCHEDULE_NOW)) //
                .largeTxn() //
                .withTenantType(TenantType.CUSTOMER) //
                .build();
    }
}
