package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.ListSegmentDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.ListSegment;

@Component("segmentDao")
public class LIstSegmentDaoImpl extends BaseDaoImpl<ListSegment> implements ListSegmentDao {

    @Override
    protected Class<ListSegment> getEntityClass() {
        return ListSegment.class;
    }

}
