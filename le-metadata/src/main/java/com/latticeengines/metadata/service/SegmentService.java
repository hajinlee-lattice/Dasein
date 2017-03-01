package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;

public interface SegmentService {

    MetadataSegment createOrUpdateSegment(MetadataSegment segment);

    Boolean deleteSegmentByName(String segmentName);

    List<MetadataSegment> getSegments();

    MetadataSegment findByName(String name);
}
