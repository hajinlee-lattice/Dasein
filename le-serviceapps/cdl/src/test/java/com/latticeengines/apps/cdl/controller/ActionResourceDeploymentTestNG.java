package com.latticeengines.apps.cdl.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.apps.core.entitymgr.ActionEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;

public class ActionResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ActionResourceDeploymentTestNG.class);

    @Inject
    private ActionEntityMgr actionEntityMgr;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    private static final String ACTION_INITIATOR = "test@lattice-engines.com";

    private static final Long OWNER_ID = 10002L;

    private static final Long NEW_OWNER_ID = 10003L;

    private List<Action> actions;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();

        log.info("internalResourceHostPort is " + internalResourceHostPort);
        actions = new ArrayList<>();
        Action actionWithOwner = generateCDLImportAction();
        actionWithOwner.setOwnerId(OWNER_ID);
        Action actionWithoutOwner1 = generateCDLImportAction();
        Action actionWithoutOwner2 = generateCDLImportAction();
        actions.add(actionWithOwner);
        actions.add(actionWithoutOwner1);
        actions.add(actionWithoutOwner2);
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

    @Test(groups = "deployment-app")
    public void testCreate() {
        for (Action action : actions) {
            createAction(action);
        }
        List<Action> retrievedActions = findAll();
        Assert.assertEquals(retrievedActions.size(), 3);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCreate" })
    public void testGet() {
        List<Action> actionsWithOwner = findByOwnerId(OWNER_ID);
        Assert.assertEquals(actionsWithOwner.size(), 1);
        Action retrievedAction = findByPid(actionsWithOwner.get(0).getPid());
        Assert.assertNotNull(retrievedAction);
        List<Action> actionsWithoutOwner = findByOwnerId(null);
        Assert.assertEquals(actionsWithoutOwner.size(), 2);

        // test internal proxy for getting jobs
        List<Long> actionPids = findAll().stream().map(Action::getPid).collect(Collectors.toList());
        List<Job> retrievedJobs = findJobsBasedOnActionIdsAndType(actionPids, ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Assert.assertTrue(CollectionUtils.isEmpty(retrievedJobs));
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testGet" })
    public void testUpdate() {
        Action actionWithoutOwner = findByOwnerId(null).get(0);
        actionWithoutOwner.setOwnerId(OWNER_ID);
        update(actionWithoutOwner);
        List<Action> actionsWithOwner = findByOwnerId(OWNER_ID);
        Assert.assertEquals(actionsWithOwner.size(), 2);
        List<Action> actionsWithoutOwner = findByOwnerId(null);
        Assert.assertEquals(actionsWithoutOwner.size(), 1);

        List<Long> allActionIds = findAll().stream().map(Action::getPid).collect(Collectors.toList());
        updateOwnerIdIn(NEW_OWNER_ID, allActionIds);
        actions = findByOwnerId(NEW_OWNER_ID);
        Assert.assertEquals(actions.size(), 3);
        actions = findByPidIn(allActionIds);
        Assert.assertEquals(actions.size(), 3);
        log.info(String.format("All actions are %s", Arrays.toString(actions.toArray())));
    }

    private void createAction(Action action) {
        actionProxy.createAction(CustomerSpace.parse(mainTestTenant.getId()).toString(), action);
    }

    private List<Action> findAll() {
        return actionProxy.getActions(CustomerSpace.parse(mainTestTenant.getId()).toString());
    }

    private List<Action> findByOwnerId(Long ownerId) {
        return actionProxy.getActionsByOwnerId(CustomerSpace.parse(mainTestTenant.getId()).toString(), ownerId);
    }

    private void update(Action action) {
        actionProxy.updateAction(CustomerSpace.parse(mainTestTenant.getId()).toString(), action);
    }

    private Action findByPid(Long pid) {
        return actionEntityMgr.findByPid(pid);
    }

    private void updateOwnerIdIn(Long ownerId, List<Long> actionPids) {
        actionProxy.patchOwnerIdByPids(CustomerSpace.parse(mainTestTenant.getId()).toString(), ownerId, actionPids);
    }

    private List<Action> findByPidIn(List<Long> actionPids) {
        return actionProxy.getActionsByPids(CustomerSpace.parse(mainTestTenant.getId()).toString(), actionPids);
    }

    private List<Job> findJobsBasedOnActionIdsAndType(List<Long> actionPids, ActionType actionType) {
        return plsInternalProxy.findJobsBasedOnActionIdsAndType(
                CustomerSpace.parse(mainTestTenant.getId()).toString(), actionPids, actionType);
    }

}
