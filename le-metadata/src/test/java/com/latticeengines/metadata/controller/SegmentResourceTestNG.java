package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

public class SegmentResourceTestNG extends MetadataFunctionalTestNGBase {

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional")
    public void createSegment() {
        String url = String.format("%s/metadata/customerspaces/%s/segments/SEGMENT1?tableName=%s", //
                getRestAPIHostPort(), CUSTOMERSPACE1, TABLE1);
        MetadataSegment response = restTemplate.postForObject(url, null, MetadataSegment.class);
        assertEquals(response.getName(), "SEGMENT1");
        assertEquals(response.getTableName(), TABLE1);
    }
}