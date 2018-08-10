package com.latticeengines.pls.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;

public interface MetadataSegmentService {
    List<MetadataSegment> getSegments();

    MetadataSegment getSegmentByName(String name);

    MetadataSegment getSegmentByName(String name, boolean shouldTransateForFrontend);

    MetadataSegmentDTO getSegmentDTOByName(String name, boolean shouldTransateForFrontend);

    MetadataSegment createOrUpdateSegment(MetadataSegment segment);

    void deleteSegmentByName(String name);

    Map<CDLObjectTypes, List<String>> getDependencies(String segmentName);
}
