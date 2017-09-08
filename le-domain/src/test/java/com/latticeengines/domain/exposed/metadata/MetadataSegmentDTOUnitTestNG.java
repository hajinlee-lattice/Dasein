package com.latticeengines.domain.exposed.metadata;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class MetadataSegmentDTOUnitTestNG {

    private static final String DISPLAY_NAME = "segment";
    private static final long PRIMARY_KEY = 1l;

    @Test(groups = "unit")
    public void testSerialization() {
        MetadataSegmentDTO segmentDTO = new MetadataSegmentDTO();
        MetadataSegment segment = new MetadataSegment();
        segment.setPid(PRIMARY_KEY);
        segment.setDisplayName(DISPLAY_NAME);
        segmentDTO.setMetadataSegment(segment);
        segmentDTO.setPrimaryKey(segment.getPid());

        String str = segmentDTO.toString();
        System.out.println("str is " + str);
        MetadataSegmentDTO deserializedObject = JsonUtils.deserialize(str, MetadataSegmentDTO.class);
        Assert.assertEquals(deserializedObject.getMetadataSegment().getDisplayName(), DISPLAY_NAME);
        Assert.assertEquals(deserializedObject.getPrimaryKey().longValue(), PRIMARY_KEY);
    }

}
