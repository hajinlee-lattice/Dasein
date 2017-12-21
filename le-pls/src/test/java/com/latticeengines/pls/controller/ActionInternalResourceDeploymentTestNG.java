package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.pls.entitymanager.ActionEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public class ActionInternalResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ActionInternalResourceDeploymentTestNG.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private ActionEntityMgr actionEntityMgr;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    private static final String ACTION_INITIATOR = "test@lattice-engines.com";

    private static final String OWNER_ID = "application_10002";

    private List<Action> actions;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        System.out.println("internalResourceHostPort is " + internalResourceHostPort);
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        setupTestEnvironmentWithOneTenant();
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
        action.setTrackingId(String.valueOf(trackingId));
        return action;
    }

    @Test(groups = "deployment")
    public void testCreate() {
        for (Action action : actions) {
            createAction(action);
        }
        List<Action> retrievedActions = findAll();
        Assert.assertEquals(retrievedActions.size(), 3);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testCreate" })
    public void testGet() {
        List<Action> actionsWithOwner = findByOwnerId(OWNER_ID);
        Assert.assertEquals(actionsWithOwner.size(), 1);
        Action retrievedAction = findByPid(actionsWithOwner.get(0).getPid());
        Assert.assertNotNull(retrievedAction);
        List<Action> actionsWithoutOwner = findByOwnerId(null);
        Assert.assertEquals(actionsWithoutOwner.size(), 2);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGet" })
    public void testUpdate() {
        Action actionWithoutOwner = findByOwnerId(null).get(0);
        actionWithoutOwner.setOwnerId(OWNER_ID);
        update(actionWithoutOwner);
        List<Action> actionsWithOwner = findByOwnerId(OWNER_ID);
        Assert.assertEquals(actionsWithOwner.size(), 2);
        List<Action> actionsWithoutOwner = findByOwnerId(null);
        Assert.assertEquals(actionsWithoutOwner.size(), 1);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        List<Action> retrievedActions = findAll();
        for (Action action : retrievedActions) {
            log.info("delete action with pid " + action.getPid());
            delete(action);
        }
        retrievedActions = findAll();
        Assert.assertEquals(retrievedActions.size(), 0);
    }

    protected void createAction(Action action) {
        internalResourceRestApiProxy.createAction(CustomerSpace.parse(mainTestTenant.getId()).toString(), action);
    }

    protected List<Action> findAll() {
        return internalResourceRestApiProxy.getAllActions(CustomerSpace.parse(mainTestTenant.getId()).toString());
    }

    protected List<Action> findByOwnerId(String ownerId) {
        return internalResourceRestApiProxy.getActionsByOwnerId(CustomerSpace.parse(mainTestTenant.getId()).toString(),
                ownerId);
    }

    protected void update(Action action) {
        internalResourceRestApiProxy.updateAction(CustomerSpace.parse(mainTestTenant.getId()).toString(), action);
    }

    protected void delete(Action action) {
        internalResourceRestApiProxy.deleteAction(CustomerSpace.parse(mainTestTenant.getId()).toString(),
                action.getPid());
    }

    protected Action findByPid(Long pid) {
        return actionEntityMgr.findByPid(pid);
    }

}
