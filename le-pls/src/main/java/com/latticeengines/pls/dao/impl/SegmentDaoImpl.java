package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.pls.dao.SegmentDao;

@Component("segmentDao")
public class SegmentDaoImpl extends BaseDaoImpl<Segment> implements SegmentDao {

    @Override
    protected Class<Segment> getEntityClass() {
        return Segment.class;
    }

}
