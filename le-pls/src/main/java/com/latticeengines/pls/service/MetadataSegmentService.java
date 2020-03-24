package com.latticeengines.pls.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface MetadataSegmentService {
    List<MetadataSegment> getSegments();

    MetadataSegment getSegmentByName(String name);

    MetadataSegment getSegmentByName(String name, boolean shouldTranslateForFrontend);

    MetadataSegmentDTO getSegmentDTOByName(String name, boolean shouldTranslateForFrontend);

    MetadataSegment createOrUpdateSegment(MetadataSegment segment);

    Map<BusinessEntity, Long> updateSegmentCounts(String segmentName);

    void deleteSegmentByName(String name, boolean hardDelete);

    void revertDeleteSegment(String segmentName);

    List<String> getAllDeletedSegments();

    Map<String, List<String>> getDependencies(String segmentName);

    UIAction getDependenciesModelAndView(String segmentName);

    UIAction deleteSegmentByNameModelAndView(String segmentName, boolean hardDelete);
}
