package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import java.util.List;

public interface SegmentService {

    MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment segment);

    List<MetadataSegment> getSegments();

    MetadataSegment findByName(String name);
}
