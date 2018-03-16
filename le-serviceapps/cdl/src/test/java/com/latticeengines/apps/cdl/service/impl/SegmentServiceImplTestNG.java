package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.AttributeLookup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class SegmentServiceImplTestNG extends CDLFunctionalTestNGBase {


    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDummySegment();
    }

    @Test(groups = "functional")
    public void testFindDependingAttributes() {
        List<MetadataSegment> segments = new ArrayList<>();
        segments.add(testSegment);

        List<AttributeLookup> attributeLookups = segmentService.findDependingAttributes(segments);
        assertNotNull(attributeLookups);
        assertEquals(attributeLookups.size(), 8);
    }

    @Test(groups = "functional")
    public void testFindDependingSegments() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Contact.CompanyName");

        List<MetadataSegment> segments = segmentService.findDependingSegments(mainCustomerSpace, attributes);
        assertNotNull(segments);
        assertEquals(segments.size(), 1);
        assertEquals(segments.get(0).getDisplayName(), SEGMENT_NAME);
    }
}
