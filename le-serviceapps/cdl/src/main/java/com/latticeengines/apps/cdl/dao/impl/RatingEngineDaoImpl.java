package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.RatingEngineDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.RatingEngine;

@Component("ratingEngineDao")
public class RatingEngineDaoImpl extends BaseDaoImpl<RatingEngine> implements RatingEngineDao {

    @Override
    protected Class<RatingEngine> getEntityClass() {
        return RatingEngine.class;
    }

    @Override
    public RatingEngine findById(String id) {
        return super.findByField("ID", id);
    }

    @Override
    public List<RatingEngine> findAllByTypeAndStatus(String type, String status) {
        if (type == null && status == null) {
            return super.findAll();
        } else if (type == null && status != null) {
            return super.findAllByFields("status", status);
        } else if (type != null && status == null) {
            return super.findAllByFields("type", type);
        } else {
            return super.findAllByFields("type", type, "status", status);
        }
    }

}
