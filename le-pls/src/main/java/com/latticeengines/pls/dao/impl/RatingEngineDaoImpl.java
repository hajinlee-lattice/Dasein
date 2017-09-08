package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.pls.dao.RatingEngineDao;

@Component("ratingEngineDao")
public class RatingEngineDaoImpl extends BaseDaoImpl<RatingEngine> implements RatingEngineDao {

    @Override
    protected Class<RatingEngine> getEntityClass() {
        return RatingEngine.class;
    }

    private Class<MetadataSegment> getMetadataSegmentClass() {
        return MetadataSegment.class;
    }

    @Override
    public RatingEngine findById(String id) {
        return super.findByField("ID", id);
    }

    @Override
    public MetadataSegment findMetadataSegmentByName(String name) {
        // TODO Auto-generated method stub
        return null;
    }

}
