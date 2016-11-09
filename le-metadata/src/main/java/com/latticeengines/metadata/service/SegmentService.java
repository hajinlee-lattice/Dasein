package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;

public interface SegmentService {

    MetadataSegment createSegment(String customerSpace, String name, String tableName);

    List<MetadataSegment> getSegments();

    MetadataSegment findByName(String name);
}
