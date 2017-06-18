package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentProperty;
import com.latticeengines.metadata.dao.SegmentPropertyDao;

@Component("segmentPropertyDao")
public class SegmentPropertyDaoImpl extends BaseMetadataPropertyDaoImpl<MetadataSegmentProperty, MetadataSegment>
        implements SegmentPropertyDao {

    @Override
    protected Class<MetadataSegmentProperty> getEntityClass() {
        return MetadataSegmentProperty.class;
    }

}
