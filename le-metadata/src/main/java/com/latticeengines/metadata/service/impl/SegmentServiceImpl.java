package com.latticeengines.metadata.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.metadata.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.metadata.service.SegmentService;

@Component("segmentService")
public class SegmentServiceImpl implements SegmentService {

    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Override
    public MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment segment) {
        segmentEntityMgr.createOrUpdate(segment);
        return segmentEntityMgr.findByName(segment.getName());
    }

    @Override
    public Boolean deleteSegmentByName(String customerSpace, String segmentName) {
        MetadataSegment segment = segmentEntityMgr.findByName(segmentName);
        if (segment == null) {
            return false;
        }
        segmentEntityMgr.delete(segment);
        return true;
    }

    @Override
    public List<MetadataSegment> getSegments(String customerSpace) {
        String collectionName = dataCollectionEntityMgr.getOrCreateDefaultCollection().getName();
        return segmentEntityMgr.findAllInCollection(collectionName);
    }

    @Override
    public List<MetadataSegment> getSegments(String customerSpace, String collectionName) {
        List<MetadataSegment> segments = segmentEntityMgr.findAll();
        if (segments == null || segments.isEmpty()) {
            return Collections.emptyList();
        }
        return segments.stream() //
                .filter(segment -> collectionName.equals(segment.getDataCollection().getName())) //
                .collect(Collectors.toList());
    }

    @Override
    public MetadataSegment findByName(String customerSpace, String name) {
        return segmentEntityMgr.findByName(name);
    }

    @Override
    public MetadataSegment findMaster(String customerSpace, String collectionName) {
        return segmentEntityMgr.findMasterSegment(collectionName);
    }

    @Override
    public StatisticsContainer getStats(String customerSpace, String segmentName, DataCollection.Version version) {
        if (version == null) {
            // by default get from active version
            version = dataCollectionEntityMgr.getActiveVersion();
        }
        return statisticsContainerEntityMgr.findInSegment(segmentName, version);
    }

    @Override
    public void upsertStats(String customerSpace, String segmentName, StatisticsContainer statisticsContainer) {
        segmentEntityMgr.upsertStats(segmentName, statisticsContainer);
    }

    @Override
    public void deleteAllSegments(String customerSpace) {
        List<MetadataSegment> segments = getSegments(customerSpace);
        for (MetadataSegment segment : segments) {
            deleteSegmentByName(customerSpace, segment.getName());
        }
    }

}
