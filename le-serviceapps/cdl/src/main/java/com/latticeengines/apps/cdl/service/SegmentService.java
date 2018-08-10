package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface SegmentService {

    MetadataSegment findByName(String name);

    MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment segment);

    Boolean deleteSegmentByName(String customerSpace, String segmentName, boolean ignoreDependencyCheck);

    List<MetadataSegment> getSegments(String customerSpace);

    List<MetadataSegment> getSegments(String customerSpace, String collectionName);

    MetadataSegment findByName(String customerSpace, String name);

    MetadataSegment findMaster(String customerSpace, String collectionName);

    StatisticsContainer getStats(String customerSpace, String segmentName, DataCollection.Version version);

    void upsertStats(String customerSpace, String segmentName, StatisticsContainer statisticsContainer);

    void deleteAllSegments(String customerSpace, boolean ignoreDependencyCheck);

    Map<BusinessEntity, Long> updateSegmentCounts(String segmentName);

    List<AttributeLookup> findDependingAttributes(List<MetadataSegment> metadataSegments);

    List<MetadataSegment> findDependingSegments(String customerSpace, List<String> attributes);

    void verifySegmentCyclicDependency(MetadataSegment metadataSegment);

    Map<CDLObjectTypes, List<String>> getDependencies(String customerSpace, String segmentName) throws Exception;
}
