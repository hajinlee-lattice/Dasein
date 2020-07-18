package com.latticeengines.apps.core.entitymgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.entitymgr.ActionEntityMgr;
import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ActivityMetricsActionConfiguration;
import com.latticeengines.domain.exposed.pls.AttrConfigLifeCycleChangeConfiguration;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.SegmentActionConfiguration;

public class ActionEntityMgrImplTestNG extends ServiceAppsFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ActionEntityMgrImplTestNG.class);

    @Inject
    private ActionEntityMgr actionEntityMgr;

    @Inject
    private JdbcTemplate jdbcTemplate;

    private static final String ACTION_INITIATOR = "test@lattice-engines.com";

    private static final Long OWNER_ID = 10002L;

    private static final Long NEW_OWNER_ID = 10003L;

    private List<Action> actions;
    private List<Long> jobPids;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        actions = new ArrayList<>();
        Action actionWithOwner = generateCDLImportAction();
        actionWithOwner.setOwnerId(OWNER_ID);
        actionWithOwner.setActionConfiguration(generateActionConfig(0));
        Action actionWithoutOwner1 = generateCDLImportAction();
        actionWithoutOwner1.setActionConfiguration(generateActionConfig(1));
        Action actionWithoutOwner2 = generateCDLImportAction();
        actionWithoutOwner2.setActionConfiguration(generateActionConfig(2));
        actions.add(actionWithOwner);
        actions.add(actionWithoutOwner1);
        actions.add(actionWithoutOwner2);
        jobPids = new ArrayList<>();
        jobPids.add(actionWithOwner.getTrackingPid());
        jobPids.add(actionWithoutOwner1.getTrackingPid());
        jobPids.add(actionWithoutOwner2.getTrackingPid());
    }

    private Action generateCDLImportAction() {
        Action action = new Action();
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action.setActionInitiator(ACTION_INITIATOR);
        action.setTenant(mainTestTenant);
        Random r = new Random();
        int trackingId = r.ints(0, (10000 + 1)).findFirst().getAsInt();
        action.setTrackingPid((long) trackingId);
        return action;
    }

    private ActionConfiguration generateActionConfig(int n) {
        ActionConfiguration ac;
        switch (n) {
            case 0:
                ac = new SegmentActionConfiguration();
                ((SegmentActionConfiguration) ac).setSegmentName("Segment_abc");
                break;
            case 1:
                ac = new RatingEngineActionConfiguration();
                ((RatingEngineActionConfiguration) ac).setRatingEngineId("RatingEngine_abc");
                ((RatingEngineActionConfiguration) ac)
                        .setSubType(RatingEngineActionConfiguration.SubType.RULE_MODEL_BUCKET_CHANGE);
                ((RatingEngineActionConfiguration) ac).setModelId("RatingModel_abc");
                break;
            case 2:
                ac = new ActivityMetricsActionConfiguration();
                ((ActivityMetricsActionConfiguration) ac).setActivated(Collections.emptyList());
                ((ActivityMetricsActionConfiguration) ac).setUpdated(Collections.emptyList());
                ((ActivityMetricsActionConfiguration) ac).setDeactivated(Collections.emptyList());
                break;
            case 3:
                ac = new AttrConfigLifeCycleChangeConfiguration();
                ((AttrConfigLifeCycleChangeConfiguration) ac)
                        .setSubType(AttrConfigLifeCycleChangeConfiguration.SubType.ACTIVATION);
                ((AttrConfigLifeCycleChangeConfiguration) ac).setAttrNums(1000L);
                ((AttrConfigLifeCycleChangeConfiguration) ac).setCategoryName("Category_abc");
                break;
            default:
                ac = null;
        }
        return ac;
    }

    @Test(groups = "functional")
    public void testCreate() {
        for (Action action : actions) {
            System.out.println(JsonUtils.serialize(action));
            createAction(action);
        }
        List<Action> retrievedActions = findAll();
        Assert.assertEquals(retrievedActions.size(), 3);
    }

    @Test(groups = "functional", dependsOnMethods = {"testCreate"})
    public void testGetActionByJodPids() {
        List<Action> actions = getActionsByJobPids(jobPids);
        Assert.assertNotNull(actions);
    }

    @Test(groups = "functional", dependsOnMethods = {"testCreate"})
    public void testCancel() {
        for (Action action : actions) {
            System.out.println(JsonUtils.serialize(action));
            cancelAction(action.getPid());
        }
        List<Action> retrievedActions = findAll();
        for (Action action : retrievedActions) {
            System.out.println(JsonUtils.serialize(action));
            if (action.getOwnerId() == null)
                Assert.assertEquals(action.getActionStatus(), ActionStatus.CANCELED);
        }
    }

    @Test(groups = "functional", dependsOnMethods = {"testCancel"})
    public void testGet() {
        List<Action> actionsWithOwner = findByOwnerId(OWNER_ID);
        Assert.assertEquals(actionsWithOwner.size(), 1);
        Action retrievedAction = findByPid(actionsWithOwner.get(0).getPid());
        Assert.assertNotNull(retrievedAction);
        Assert.assertNotNull(actionsWithOwner.get(0).getActionConfiguration());
        Assert.assertTrue(actionsWithOwner.get(0).getActionConfiguration() instanceof SegmentActionConfiguration);
        List<Action> actionsWithoutOwner = findByOwnerId(null);
        Assert.assertEquals(actionsWithoutOwner.size(), 2);
        Assert.assertNotNull(actionsWithoutOwner.get(0).getActionConfiguration());
        Assert.assertNotNull(actionsWithoutOwner.get(1).getActionConfiguration());
        Assert.assertTrue(
                actionsWithoutOwner.get(0).getActionConfiguration() instanceof RatingEngineActionConfiguration);
        Assert.assertTrue(
                actionsWithoutOwner.get(1).getActionConfiguration() instanceof ActivityMetricsActionConfiguration);
    }

    @Test(groups = "functional", dependsOnMethods = {"testGet"})
    public void testUpdate() {
        Action actionWithoutOwner = findByOwnerId(null).get(0);
        actionWithoutOwner.setOwnerId(OWNER_ID);
        update(actionWithoutOwner);
        List<Action> actions = findByOwnerId(OWNER_ID);
        Assert.assertEquals(actions.size(), 2);
        List<Action> actionsWithoutOwner = findByOwnerId(null);
        Assert.assertEquals(actionsWithoutOwner.size(), 1);

        List<Long> allActionIds = findAll().stream().map(action -> action.getPid()).collect(Collectors.toList());
        updateOwnerIdIn(NEW_OWNER_ID, allActionIds);
        actions = findByOwnerId(NEW_OWNER_ID);
        Assert.assertEquals(actions.size(), 3);
        actions = findByPidIn(allActionIds);
        Assert.assertEquals(actions.size(), 3);
        log.info(String.format("All actions are %s", Arrays.toString(actions.toArray())));
    }

    @Test(groups = "functional", dependsOnMethods = {"testUpdate"})
    public void testDelete() {
        for (Action action : actions) {
            delete(action);
        }
        List<Action> retrievedActions = findAll();
        Assert.assertEquals(retrievedActions.size(), 0);
    }

    @Test(groups = "functional", dependsOnMethods = {"testDelete"})
    public void testGetWithNameQuery() {
        Long ownerId = createFakeWorkflow(mainTestTenant.getPid());
        Action action = generateCDLImportAction();
        ImportActionConfiguration ac = new ImportActionConfiguration();
        String dftUUID = NamingUtils.uuid("DataFeedTask");
        ac.setDataFeedTaskId(dftUUID);
        ac.setImportCount(100L);
        action.setActionConfiguration(ac);
        createAction(action);

        List<Long> actionPidList =
                actionEntityMgr.findPidWithoutOwnerByTypeAndStatusAndConfig(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW,
                        ActionStatus.ACTIVE, dftUUID);
        Assert.assertEquals(CollectionUtils.size(actionPidList), 1);
        Assert.assertEquals(actionPidList.get(0), action.getPid());

        action.setOwnerId(ownerId);
        updateOwnerIdIn(ownerId, Collections.singletonList(action.getPid()));

        List<ActionConfiguration> configList =
                actionEntityMgr.findConfigByTypeAndOwnerType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW, "processAnalyzeWorkflow");
        Assert.assertNotNull(configList);
        Assert.assertTrue(configList.get(0) instanceof ImportActionConfiguration);
        ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) configList.get(0);
        Assert.assertEquals(importActionConfiguration.getDataFeedTaskId(), dftUUID);
    }

    private Long createFakeWorkflow(long tenantPid) {
        int rand = new Random(System.currentTimeMillis()).nextInt(10000);
        String appId = String.format("application_%d_%04d", System.currentTimeMillis(), rand);

        String sql = "INSERT INTO `WORKFLOW_JOB` ";
        sql += "(`TENANT_ID`, `FK_TENANT_ID`, `USER_ID`, `APPLICATION_ID`, `INPUT_CONTEXT`, `START_TIME`, `TYPE`) VALUES ";
        sql += String.format("(%d, %d, 'DEFAULT_USER', '%s', '{}', NOW(), '%s')", tenantPid, tenantPid, appId, "processAnalyzeWorkflow");
        jdbcTemplate.execute(sql);

        sql = "SELECT `PID` FROM `WORKFLOW_JOB` WHERE ";
        sql += String.format("`TENANT_ID` = %d", tenantPid);
        sql += String.format(" AND `APPLICATION_ID` = '%s'", appId);
        long pid = jdbcTemplate.queryForObject(sql, Long.class);
        log.info("Created a fake workflow " + pid);
        return pid;
    }

    protected void createAction(Action action) {
        actionEntityMgr.create(action);
    }

    protected void cancelAction(Long actionPid) {
        actionEntityMgr.cancel(actionPid);
    }

    protected List<Action> findAll() {
        return actionEntityMgr.findAll();
    }

    protected List<Action> findByOwnerId(Long ownerId) {
        return actionEntityMgr.findByOwnerId(ownerId, null);
    }

    protected Action findByPid(Long pid) {
        return actionEntityMgr.findByPid(pid);
    }

    protected void update(Action action) {
        actionEntityMgr.createOrUpdate(action);
    }

    protected void delete(Action action) {
        actionEntityMgr.delete(action);
    }

    protected void updateOwnerIdIn(Long ownerId, List<Long> actionPids) {
        actionEntityMgr.updateOwnerIdIn(ownerId, actionPids);
    }

    protected List<Action> findByPidIn(List<Long> actionPids) {
        return actionEntityMgr.findByPidIn(actionPids);
    }

    protected List<Action> getActionsByJobPids(List<Long> jobPids) { return actionEntityMgr.getActionsByJobPids(jobPids); }
}
