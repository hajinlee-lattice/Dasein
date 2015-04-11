package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.SegmentEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class SegmentEntityMgrImplTestNG extends PlsFunctionalTestNGBase {
    
    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private SegmentEntityMgr segmentEntityMgr;
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    private Tenant tenant1;
    private Tenant tenant2;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupDb("TENANT1", "TENANT2");
        segmentEntityMgr.deleteAll();
        
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
        segmentEntityMgr.create(segment1);

        Segment segment2 = new Segment();
        segment2.setModelId(summariesForTenant2.get(0).getId());
        segment2.setName("US");
        segment2.setPriority(1);
        segment2.setTenant(tenant2);
        segmentEntityMgr.create(segment2);
        
        assertEquals(segmentEntityMgr.getAll().size(), 2);
    }
    
    @Test(groups = "functional")
    public void findAll() {
        setupSecurityContext(tenant1);
        List<Segment> segments = segmentEntityMgr.findAll();
        assertEquals(segments.size(), 1);
    }
}
