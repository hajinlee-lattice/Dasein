package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.SegmentEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;

public class SegmentEntityMgrImplTestNG extends PlsFunctionalTestNGBaseDeprecated {
    
    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TenantService tenantService;
    
    private Tenant tenant1;
    private Tenant tenant2;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupDbWithMarketoSMB("TENANT1", "TENANT1");
        setupDbWithEloquaSMB("TENANT2", "TENANT2");
        
        tenant1 = tenantEntityMgr.findByTenantId("TENANT1");
        tenant2 = tenantEntityMgr.findByTenantId("TENANT2");
        
        setupSecurityContext(tenant1);
        List<ModelSummary> summariesForTenant1 = modelSummaryEntityMgr.findAll();
        setupSecurityContext(tenant2);
        List<ModelSummary> summariesForTenant2 = modelSummaryEntityMgr.findAll();
        
        Segment segment1 = new Segment();
        segment1.setModelId(summariesForTenant1.get(0).getId());
        segment1.setName("SMB");
        segment1.setPriority(1);
        segment1.setTenant(tenant1);
        setupSecurityContext(tenant1);
        for (Segment segment: segmentEntityMgr.findAll()) {
            segmentEntityMgr.delete(segment);
        }
        segmentEntityMgr.create(segment1);

        Segment segment2 = new Segment();
        segment2.setModelId(summariesForTenant2.get(0).getId());
        segment2.setName("US");
        segment2.setPriority(2);
        segment2.setTenant(tenant2);
        setupSecurityContext(tenant2);
        for (Segment segment: segmentEntityMgr.findAll()) {
            segmentEntityMgr.delete(segment);
        }
        segmentEntityMgr.create(segment2);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        tenantService.discardTenant(tenant1);
        tenantService.discardTenant(tenant2);
    }
    
    @Test(groups = "functional")
    public void findAll() {
        setupSecurityContext(tenant1);
        List<Segment> segments = segmentEntityMgr.findAll();
        assertEquals(segments.size(), 1);
    }
    
    @Test(groups = "functional")
    public void getAll() {
        assertEquals(segmentEntityMgr.getAll().size(), 2);
    }

    @Test(groups = "functional")
    public void findByName() {
        setupSecurityContext(tenant2);
        Segment segment = segmentEntityMgr.findByName("US");
        assertNotNull(segment);
        assertEquals(segment.getPriority().intValue(), 2);
    }

}
