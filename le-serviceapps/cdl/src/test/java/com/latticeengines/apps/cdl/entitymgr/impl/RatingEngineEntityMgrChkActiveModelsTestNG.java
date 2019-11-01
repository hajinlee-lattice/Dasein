package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.UUID;

import javax.inject.Inject;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.provision.impl.CDLComponent;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;

public class RatingEngineEntityMgrChkActiveModelsTestNG extends CDLFunctionalTestNGBase {
    private static final Logger log = LoggerFactory
            .getLogger(RatingEngineEntityMgrChkActiveModelsTestNG.class);

    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String RATING_ENGINE_NEW_NOTE = "This is a Rating Engine that covers East Asia market";

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private BatonService batonService;

    @Inject
    private RatingEngineService ratingEngineService;

    private RatingEngine ratingEngineTest1;

    private RatingEngine ratingEngineTest2;

    private RatingEngine ratingEngineTest3;

    private RatingEngine createdRatingEngine;

    private RatingEngine crossSellRatingEngine;

    private RatingEngine customEventRatingEngine;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();
        ratingEngineTest1 = new RatingEngine();
        ratingEngineTest1.setSegment(testSegment);
        ratingEngineTest1.setCreatedBy(CREATED_BY);
        ratingEngineTest1.setUpdatedBy(CREATED_BY);
        ratingEngineTest1.setType(RatingEngineType.RULE_BASED);
        ratingEngineTest1.setNote(RATING_ENGINE_NOTE);
        ratingEngineTest1.setId(UUID.randomUUID().toString());

        ratingEngineTest2 = new RatingEngine();
        ratingEngineTest2.setSegment(testSegment);
        ratingEngineTest2.setCreatedBy(CREATED_BY);
        ratingEngineTest2.setUpdatedBy(CREATED_BY);
        ratingEngineTest2.setType(RatingEngineType.RULE_BASED);
        ratingEngineTest2.setNote(RATING_ENGINE_NOTE);
        ratingEngineTest2.setId(UUID.randomUUID().toString());

        crossSellRatingEngine = new RatingEngine();
        crossSellRatingEngine.setSegment(testSegment);
        crossSellRatingEngine.setCreatedBy(CREATED_BY);
        crossSellRatingEngine.setUpdatedBy(CREATED_BY);
        crossSellRatingEngine.setType(RatingEngineType.CROSS_SELL);
        crossSellRatingEngine.setNote(RATING_ENGINE_NOTE);
        crossSellRatingEngine.setId(UUID.randomUUID().toString());

        customEventRatingEngine = new RatingEngine();
        customEventRatingEngine.setSegment(testSegment);
        customEventRatingEngine.setCreatedBy(CREATED_BY);
        customEventRatingEngine.setUpdatedBy(CREATED_BY);
        customEventRatingEngine.setType(RatingEngineType.CUSTOM_EVENT);
        customEventRatingEngine.setNote(RATING_ENGINE_NOTE);
        customEventRatingEngine.setId(UUID.randomUUID().toString());

        ratingEngineTest3 = new RatingEngine();
        ratingEngineTest3.setSegment(testSegment);
        ratingEngineTest3.setCreatedBy(CREATED_BY);
        ratingEngineTest3.setUpdatedBy(CREATED_BY);
        ratingEngineTest3.setType(RatingEngineType.RULE_BASED);
        ratingEngineTest3.setNote(RATING_ENGINE_NOTE);
        ratingEngineTest3.setId(UUID.randomUUID().toString());

        ActionContext.remove();
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        ActionContext.remove();
    }

    @Test(groups = "functional")
    public void create() {
        createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(ratingEngineTest1);
        log.info("1Rating Engine is " + createdRatingEngine.toString());
        Assert.assertNotNull(createdRatingEngine);
        createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(crossSellRatingEngine);
        log.info("2Rating Engine is " + createdRatingEngine.toString());
        Assert.assertNotNull(createdRatingEngine);
        createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(customEventRatingEngine);
        log.info("3Rating Engine is " + createdRatingEngine.toString());
        Assert.assertNotNull(createdRatingEngine);
        createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(ratingEngineTest2);
        log.info("4Rating Engine is " + createdRatingEngine.toString());
        Assert.assertNotNull(createdRatingEngine);
    }

    @Test(groups = "functional")
    public void createTenant() {
        CustomerSpace space = CustomerSpace.parse(mainTestTenant.getId());
        Path path = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), space,
                CDLComponent.componentName);
        Path activeModelCntPath = path.append("ActiveModelQuotaLimit");
        Camille camille = CamilleEnvironment.getCamille();
        try {
            if (!camille.exists(activeModelCntPath)) {
                camille.create(activeModelCntPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }
            camille.set(activeModelCntPath, new Document("50"));
        } catch (Exception e) {
            System.out.println("Error Creating Zookeeper Path : " + e);
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "create", "createTenant" })
    public void update() {
        RatingEngine re = new RatingEngine();
        re.setDisplayName(RATING_ENGINE_NAME);
        re.setNote(RATING_ENGINE_NEW_NOTE);
        re.setId(ratingEngineTest1.getId());
        re.setStatus(RatingEngineStatus.ACTIVE);
        re.setScoringIteration(ratingEngineTest1.getLatestIteration());
        re.setCountsByMap(ImmutableMap.of( //
                RatingBucketName.A.getName(), 1L, //
                RatingBucketName.B.getName(), 2L, //
                RatingBucketName.C.getName(), 3L));
        RatingEngine updatedRatingEngine1 = ratingEngineEntityMgr.updateRatingEngine(re,
                ratingEngineEntityMgr.findById(ratingEngineTest1.getId()), false);
        log.info("Rating Engine after update is " + updatedRatingEngine1.toString());
        Assert.assertEquals(updatedRatingEngine1.getStatus(), RatingEngineStatus.ACTIVE);
        re = new RatingEngine();
        re.setDisplayName(RATING_ENGINE_NAME);
        re.setNote(RATING_ENGINE_NEW_NOTE);
        re.setId(ratingEngineTest2.getId());
        re.setStatus(RatingEngineStatus.ACTIVE);
        re.setScoringIteration(ratingEngineTest2.getLatestIteration());
        re.setCountsByMap(ImmutableMap.of( //
                RatingBucketName.A.getName(), 1L, //
                RatingBucketName.B.getName(), 2L, //
                RatingBucketName.C.getName(), 3L));
        RatingEngine updatedRatingEngine2 = ratingEngineEntityMgr.updateRatingEngine(re,
                ratingEngineEntityMgr.findById(ratingEngineTest2.getId()), false);
        log.info("Rating Engine after update is " + updatedRatingEngine2.toString());
        Assert.assertEquals(updatedRatingEngine2.getStatus(), RatingEngineStatus.ACTIVE);
        // check Active Rating Engine Models for Tenant
        Assert.assertEquals(ratingEngineService.getActiveRatingEnginesCount().longValue(), 2L);
        testBed.bootstrap(2);
        mainTestTenant = testBed.getTestTenants().get(1);
        mainCustomerSpace = mainTestTenant.getId();
        MultiTenantContext.setTenant(mainTestTenant);
        ratingEngineTest3.setTenant(mainTestTenant);
        createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(ratingEngineTest3);
        Assert.assertNotNull(createdRatingEngine);
        re = new RatingEngine();
        re.setDisplayName(RATING_ENGINE_NAME);
        re.setNote(RATING_ENGINE_NEW_NOTE);
        re.setId(ratingEngineTest3.getId());
        re.setStatus(RatingEngineStatus.ACTIVE);
        re.setScoringIteration(ratingEngineTest3.getLatestIteration());
        re.setCountsByMap(ImmutableMap.of( //
                RatingBucketName.A.getName(), 1L, //
                RatingBucketName.B.getName(), 2L, //
                RatingBucketName.C.getName(), 3L));
        RatingEngine updatedRatingEngine3 = ratingEngineEntityMgr.updateRatingEngine(re,
                ratingEngineEntityMgr.findById(ratingEngineTest3.getId()), false);
        log.info("Rating Engine after update is " + updatedRatingEngine3.toString());
        Assert.assertEquals(updatedRatingEngine3.getStatus(), RatingEngineStatus.ACTIVE);
        Assert.assertEquals(ratingEngineService.getActiveRatingEnginesCount().longValue(), 1L);
    }

}
