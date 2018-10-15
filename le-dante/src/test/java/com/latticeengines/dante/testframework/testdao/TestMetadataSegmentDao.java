package com.latticeengines.dante.testframework.testdao;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;


@Component("testMetadataSegmentDao")
public class TestMetadataSegmentDao extends BaseDaoImpl<MetadataSegment> {

    @Override
    protected Class<MetadataSegment> getEntityClass() {
        return MetadataSegment.class;
    }
}
