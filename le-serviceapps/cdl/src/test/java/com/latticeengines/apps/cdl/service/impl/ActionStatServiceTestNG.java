package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.ActionStatService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.domain.exposed.cdl.scheduling.ActionStat;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ActivityMetricsActionConfiguration;
import com.latticeengines.domain.exposed.pls.AttrConfigLifeCycleChangeConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.SegmentActionConfiguration;

public class ActionStatServiceTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ActionStatServiceTestNG.class);
    private static final String ACTION_INITIATOR = "test@lattice-engines.com";

    private static final Long OWNER_ID = 10002L;

    @Inject
    private ActionService actionService;

    @Inject
    private ActionStatService actionStatService;

    private List<Action> actionList;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        actionList = new ArrayList<>();
        Action action1 = generateCDLImportAction();
        action1.setOwnerId(OWNER_ID);
        action1.setActionConfiguration(generateSegmentActionConfig());
        Action actionWithoutOwner1 = generateCDLImportAction();
        actionWithoutOwner1.setActionConfiguration(generateRatingEngineActionConfig());
        Action actionWithoutOwner2 = generateCDLImportAction();
        actionWithoutOwner2.setActionConfiguration(generateActivityMetricsActionConfig());
        Action actionWithoutOwner3 = generateCDLImportAction();
        actionWithoutOwner3.setActionConfiguration(generateAttrConfigLifeCycleChangeConfig());
        Action action2 = generateCDLImportAction();
        action2.setOwnerId(OWNER_ID);
        action2.setActionConfiguration(generateSegmentActionConfig());
        Action action3 = generateCDLImportAction();
        action3.setType(ActionType.CDL_OPERATION_WORKFLOW);
        action3.setActionConfiguration(generateRatingEngineActionConfig());
        actionList.add(action1);
        actionList.add(actionWithoutOwner1);
        actionList.add(actionWithoutOwner2);
        actionList.add(actionWithoutOwner3);
        actionList.add(action2);
        actionList.add(action3);
    }

    @Test(groups = "functional")
    public void testCreate() throws Exception {
        for (Action action : actionList) {
            actionService.create(action);
            Thread.sleep(2000);
        }
        actionList = actionService.findAll();
        Assert.assertEquals(actionList.size(), 6);
    }

    @Test(groups = "functional")
    public void testGetNoOwnerActionStatsByTypes() {
        Set<ActionType> actionTypeSet = new HashSet<>();
        actionTypeSet.add(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        List<ActionStat> actionStats = actionStatService.getNoOwnerActionStatsByTypes(actionTypeSet);
        for (ActionStat actionStat : actionStats) {
            if (actionStat.getTenantPid().equals(mainTestTenant.getPid())) {
                log.info("actionStats for tenant {} is {}.", mainTestTenant.getName(), actionStat);
                Assert.assertEquals(actionStat.getFirstActionTime(), actionList.get(1).getCreated());
                Assert.assertEquals(actionStat.getLastActionTime(), actionList.get(3).getCreated());
            }
        }
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

    private ActionConfiguration generateSegmentActionConfig() {
        SegmentActionConfiguration ac = new SegmentActionConfiguration();
        ac.setSegmentName("Segment_abc");
        return ac;
    }

    private ActionConfiguration generateRatingEngineActionConfig() {
        RatingEngineActionConfiguration ac = new RatingEngineActionConfiguration();
        ac.setRatingEngineId("RatingEngine_abc");
        ac.setSubType(RatingEngineActionConfiguration.SubType.RULE_MODEL_BUCKET_CHANGE);
        ac.setModelId("RatingModel_abc");
        return ac;
    }

    private ActionConfiguration generateActivityMetricsActionConfig() {
        ActivityMetricsActionConfiguration ac = new ActivityMetricsActionConfiguration();
        ac.setActivated(Collections.emptyList());
        ac.setUpdated(Collections.emptyList());
        ac.setDeactivated(Collections.emptyList());
        return ac;
    }

    private ActionConfiguration generateAttrConfigLifeCycleChangeConfig() {
        AttrConfigLifeCycleChangeConfiguration ac = new AttrConfigLifeCycleChangeConfiguration();
        ac.setSubType(AttrConfigLifeCycleChangeConfiguration.SubType.ACTIVATION);
        ac.setAttrNums(1000L);
        ac.setCategoryName("Category_abc");
        return ac;
    }

}
