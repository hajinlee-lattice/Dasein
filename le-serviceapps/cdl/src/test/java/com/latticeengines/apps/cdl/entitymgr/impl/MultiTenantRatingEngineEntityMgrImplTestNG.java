package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.DBConnectionContext;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.security.Tenant;

public class MultiTenantRatingEngineEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MultiTenantRatingEngineEntityMgrImplTestNG.class);

    private static final String SEGMENT_NAME = "MultiTenantRESegment";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String UPDATED_BY = "lattice@lattice-engines.com";

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    private Tenant tenant1;
    private Tenant tenant2;
    private MetadataSegment segment1;
    private MetadataSegment segment2;

    @BeforeClass(groups = "functional")
    public void setup() {
        testBed.bootstrap(2);
        tenant1 = testBed.getTestTenants().get(0);
        tenant2 = testBed.getTestTenants().get(1);
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(tenant1.getId()));
        dataCollection = dataCollectionEntityMgr.createDefaultCollection();
        segment1 = createMetadataSegment(SEGMENT_NAME);
        // segment1 = createMetadataSegment(SEGMENT_NAME,
        // CustomerSpace.parse(tenant1.getId()).toString());

        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(tenant2.getId()));
        dataCollection = dataCollectionEntityMgr.createDefaultCollection();
        segment2 = createMetadataSegment(SEGMENT_NAME);
        // segment2 = createMetadataSegment(SEGMENT_NAME,
        // CustomerSpace.parse(tenant1.getId()).toString());
    }

    @Test(groups = "functional")
    public void testFindAllForMultiTenant() {
        RatingEngine ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(segment1);
        ratingEngine1.setCreatedBy(CREATED_BY);
        ratingEngine1.setUpdatedBy(UPDATED_BY);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        ratingEngine1.setNote(RATING_ENGINE_NOTE);
        ratingEngine1.setId(UUID.randomUUID().toString());
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(tenant1.getId()));
        ratingEngineEntityMgr.createRatingEngine(ratingEngine1);

        RatingEngine ratingEngine2 = new RatingEngine();
        ratingEngine2.setSegment(segment2);
        ratingEngine2.setCreatedBy(CREATED_BY);
        ratingEngine1.setUpdatedBy(UPDATED_BY);
        ratingEngine2.setType(RatingEngineType.RULE_BASED);
        ratingEngine2.setNote(RATING_ENGINE_NOTE);
        ratingEngine2.setId(UUID.randomUUID().toString());
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(tenant2.getId()));
        ratingEngineEntityMgr.createRatingEngine(ratingEngine2);

        String type = RatingEngineType.RULE_BASED.name();
        String status = RatingEngineStatus.INACTIVE.name();

        List<RatingEngine> ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertEquals(CollectionUtils.size(ratingEngineList), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(type, status);
        Assert.assertEquals(CollectionUtils.size(ratingEngineList), 1);

        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(tenant1.getId()));
        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertEquals(CollectionUtils.size(ratingEngineList), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(type, status);
        Assert.assertEquals(CollectionUtils.size(ratingEngineList), 1);

        try {
            Thread.sleep(500); // wait for writing to propagate
        } catch (InterruptedException e) {
            log.warn("Interrupted!", e);
        }

        DBConnectionContext.setReaderConnection(true);
        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertEquals(CollectionUtils.size(ratingEngineList), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(type, status);
        Assert.assertEquals(CollectionUtils.size(ratingEngineList), 1);

        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(tenant2.getId()));
        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertEquals(CollectionUtils.size(ratingEngineList), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(type, status);
        Assert.assertEquals(CollectionUtils.size(ratingEngineList), 1);
    }

}
