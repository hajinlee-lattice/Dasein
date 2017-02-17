package com.latticeengines.metadata.dao.impl;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.metadata.dao.SegmentDao;

@Component("segmentDao")
public class SegmentDaoImpl extends BaseDaoImpl<MetadataSegment> implements SegmentDao {

    @Override
    protected Class<MetadataSegment> getEntityClass() {
        return MetadataSegment.class;
    }

}
