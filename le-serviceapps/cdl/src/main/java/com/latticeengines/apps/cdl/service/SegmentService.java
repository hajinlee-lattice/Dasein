package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.UpdateSegmentCountResponse;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface SegmentService {

    MetadataSegment findByName(String name);

    MetadataSegment createOrUpdateSegment(MetadataSegment segment);

    Boolean deleteSegmentByName(String segmentName, boolean ignoreDependencyCheck,
            boolean hardDelete);

    Boolean revertDeleteSegmentByName(String segmentName);

    List<String> getAllDeletedSegments();

    List<MetadataSegment> getSegments();

    MetadataSegment findMaster(String collectionName);

    StatisticsContainer getStats(String segmentName, DataCollection.Version version);

    void upsertStats(String segmentName, StatisticsContainer statisticsContainer);

    Map<BusinessEntity, Long> updateSegmentCounts(String segmentName);

    UpdateSegmentCountResponse updateSegmentsCounts();

    void updateSegmentsCountsAsync();

    List<AttributeLookup> findDependingAttributes(List<MetadataSegment> metadataSegments);

    List<MetadataSegment> findDependingSegments(List<String> attributes);

    Map<String, List<String>> getDependencies(String segmentName) throws Exception;
}
