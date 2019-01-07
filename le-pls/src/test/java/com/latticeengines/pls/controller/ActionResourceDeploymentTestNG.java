package com.latticeengines.pls.controller;

import com.latticeengines.domain.exposed.pls.*;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Random;

public class ActionResourceDeploymentTestNG  extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ActionResourceDeploymentTestNG.class);

    @Autowired
    private ActionService actionService;

    private Action testAction;

    private Tenant tenant;

    private static final String ACTION_INITIATOR = "jxiao@lattice-engines.com";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        tenant = deploymentTestBed.getMainTestTenant();
        deploymentTestBed.loginAndAttach(TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN),
                TestFrameworkUtils.GENERAL_PASSWORD, tenant);
        MultiTenantContext.setTenant(tenant);
    }

    @Test(groups = "deployment")
    public void testCreate() {
        testAction = generateCDLImportAction();
        testAction.setActionConfiguration(generateActionConfig(0));
        testAction = actionService.create(testAction);
        Assert.assertNotNull(testAction.getActionInitiator());
        Assert.assertEquals(testAction.getActionInitiator(), ACTION_INITIATOR);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreate")
    public void testCancel() {
        restTemplate = deploymentTestBed.getRestTemplate();
        log.info("resttemplate:"+restTemplate.getInterceptors());
        Map resultMap =
                restTemplate.postForObject(getDeployedRestAPIHostPort() + "/pls/actions/cancel?actionPid=" + testAction.getPid(), null, Map.class);
        Assert.assertNotNull(resultMap);
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
            default:
                ac = null;
                break;
        }
        return ac;
    }
}
