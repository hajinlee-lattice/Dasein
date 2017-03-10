package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;

public interface MetadataSegmentService {
    List<MetadataSegment> getSegments();

    MetadataSegment getSegmentByName(String name);

    MetadataSegment createOrUpdateSegment(MetadataSegment segment);

    void deleteSegmentByName(String name);
}
