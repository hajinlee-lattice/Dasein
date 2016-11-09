package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("segmentService")
public class SegmentServiceImpl implements SegmentService {
    
    @Autowired
    private SegmentEntityMgr segmentEntityMgr;
    
    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Override
    public MetadataSegment createSegment(String customerSpace, String name, String tableName) {
        Table table = tableEntityMgr.findByName(tableName);
        
        if (table == null) {
            throw new LedpException(LedpCode.LEDP_11006, new String[] { tableName });
        }
            
        
        MetadataSegment segment = new MetadataSegment();
        segment.setName(name);
        segment.setTable(table);
        segment.setTenant(MultiTenantContext.getTenant());
        segmentEntityMgr.create(segment);
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
