package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.query.ComparisonType;
import com.latticeengines.common.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.QuerySource;
import com.latticeengines.metadata.entitymgr.QuerySourceEntityMgr;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class SegmentEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {
    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private QuerySourceEntityMgr querySourceEntityMgr;

    private MetadataSegment segment;
    private QuerySource defaultQuerySource;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(CUSTOMERSPACE1));
        defaultQuerySource = querySourceEntityMgr.createQuerySource(new ArrayList<>(), null, true);
    }

    @Test(groups = "functional")
    public void createSegment() {
        segment = new MetadataSegment();
        segment.setName("Test");
        segmentEntityMgr.createOrUpdate(segment);
    }

    @Test(groups = "functional", dependsOnMethods = "createSegment")
    public void updateSegment() {
        segment = new MetadataSegment();
        segment.setName("Test");
        segment.setRestriction(new ConcreteRestriction(false, null, ComparisonType.EQUAL, null));
        segmentEntityMgr.createOrUpdate(segment);
    }

    @Test(groups = "functional", dependsOnMethods = "updateSegment")
    public void getSegment() {
        MetadataSegment retrieved = segmentEntityMgr.findByName("Test");
        assertEquals(retrieved.getName(), segment.getName());
        assertEquals(((ConcreteRestriction) retrieved.getRestriction()).getRelation(), ComparisonType.EQUAL);
        assertTrue(retrieved.getQuerySource().isDefault());
    }

    @Test(groups = "functional", dependsOnMethods = "getSegment")
    public void getSegmentWithExplicitQuerySource() {
        MetadataSegment retrieved = segmentEntityMgr.findByName(defaultQuerySource.getName(), "Test");
        assertEquals(retrieved.getName(), segment.getName());
        assertEquals(((ConcreteRestriction) retrieved.getRestriction()).getRelation(), ComparisonType.EQUAL);
        assertTrue(retrieved.getQuerySource().isDefault());
    }

    @Test(groups = "functional", dependsOnMethods = "getSegmentWithExplicitQuerySource")
    public void deleteSegment() {
        segmentEntityMgr.delete(segment);
        assertEquals(segmentEntityMgr.findAll().size(), 0);
    }
}
