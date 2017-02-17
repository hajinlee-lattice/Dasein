package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.service.SegmentService;

@Component("segmentService")
public class SegmentServiceImpl implements SegmentService {

    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Override
    public MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment segment) {
        segmentEntityMgr.createOrUpdate(segment);
        return segment;
    }

    @Override
    public List<MetadataSegment> getSegments() {
        return segmentEntityMgr.findAll();
    }

    @Override
    public MetadataSegment findByName(String name) {
        return segmentEntityMgr.findByName(name);
    }

}
