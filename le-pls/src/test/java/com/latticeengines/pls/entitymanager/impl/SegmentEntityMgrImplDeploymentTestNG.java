package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.PdSegmentEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

public class SegmentEntityMgrImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private PdSegmentEntityMgr pdSegmentEntityMgr;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    private Tenant tenant1;
    private Tenant tenant2;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupMarketoEloquaTestEnvironment();

        tenant1 = marketoTenant;
        tenant2 = eloquaTenant;

        setupSecurityContext(tenant1);
        List<ModelSummary> summariesForTenant1 = modelSummaryProxy.findAll(tenant1.getId());
        setupSecurityContext(tenant2);
        List<ModelSummary> summariesForTenant2 = modelSummaryProxy.findAll(tenant2.getId());

        Segment segment1 = new Segment();
        segment1.setModelId(summariesForTenant1.get(0).getId());
        segment1.setName("SMB");
        segment1.setPriority(1);
        segment1.setTenant(tenant1);
        setupSecurityContext(tenant1);
        for (Segment segment : pdSegmentEntityMgr.findAll()) {
            pdSegmentEntityMgr.delete(segment);
        }
        pdSegmentEntityMgr.create(segment1);

        Segment segment2 = new Segment();
        segment2.setModelId(summariesForTenant2.get(0).getId());
        segment2.setName("US");
        segment2.setPriority(2);
        segment2.setTenant(tenant2);
        setupSecurityContext(tenant2);
        for (Segment segment : pdSegmentEntityMgr.findAll()) {
            pdSegmentEntityMgr.delete(segment);
        }
        pdSegmentEntityMgr.create(segment2);
    }

    @Test(groups = "deployment")
    public void findAll() {
        setupSecurityContext(tenant1);
        List<Segment> segments = pdSegmentEntityMgr.findAll();
        assertEquals(segments.size(), 1);
    }

    @Test(groups = "deployment")
    public void getAll() {
        assertTrue(pdSegmentEntityMgr.getAll().size() >= 2,
                "should have at least the two segments created in this test");
    }

    @Test(groups = "deployment")
    public void findByName() {
        setupSecurityContext(tenant2);
        Segment segment = pdSegmentEntityMgr.findByName("US");
        assertNotNull(segment);
        assertEquals(segment.getPriority().intValue(), 2);
    }
}
