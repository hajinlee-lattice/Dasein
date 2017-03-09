package com.latticeengines.metadata.dao.impl;

import com.latticeengines.metadata.dao.SegmentPropertyDao;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentProperty;

@Component("segmentPropertyDao")
public class SegmentPropertyDaoImpl extends BaseDaoImpl<MetadataSegmentProperty>
        implements SegmentPropertyDao {

    @Override
    protected Class<MetadataSegmentProperty> getEntityClass() {
        return MetadataSegmentProperty.class;
    }

}
