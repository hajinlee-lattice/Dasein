package com.latticeengines.pls.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.pls.entitymanager.ActionEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class ActionEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Inject
    private ActionEntityMgr actionEntityMgr;

    private static final String ACTION_INITIATOR = "test@lattice-engines.com";

    private static final Long OWNER_ID = 10002L;;

    private List<Action> actions;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneGATenant();
        setupSecurityContext(mainTestTenant);
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
        action.setTrackingId((long) trackingId);
        return action;
    }

    @Test(groups = "functional")
    public void testCreate() {
        for (Action action : actions) {
            createAction(action);
        }
        List<Action> retrievedActions = findAll();
        Assert.assertEquals(retrievedActions.size(), 3);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreate" })
    public void testGet() {
        List<Action> actionsWithOwner = findByOwnerId(OWNER_ID);
        Assert.assertEquals(actionsWithOwner.size(), 1);
        Action retrievedAction = findByPid(actionsWithOwner.get(0).getPid());
        Assert.assertNotNull(retrievedAction);
        List<Action> actionsWithoutOwner = findByOwnerId(null);
        Assert.assertEquals(actionsWithoutOwner.size(), 2);
    }

    @Test(groups = "functional", dependsOnMethods = { "testGet" })
    public void testUpdate() {
        Action actionWithoutOwner = findByOwnerId(null).get(0);
        actionWithoutOwner.setOwnerId(OWNER_ID);
        update(actionWithoutOwner);
        List<Action> actionsWithOwner = findByOwnerId(OWNER_ID);
        Assert.assertEquals(actionsWithOwner.size(), 2);
        List<Action> actionsWithoutOwner = findByOwnerId(null);
        Assert.assertEquals(actionsWithoutOwner.size(), 1);
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        for (Action action : actions) {
            delete(action);
        }
        List<Action> retrievedActions = findAll();
        Assert.assertEquals(retrievedActions.size(), 0);
    }

    protected void createAction(Action action) {
        actionEntityMgr.create(action);
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
}
