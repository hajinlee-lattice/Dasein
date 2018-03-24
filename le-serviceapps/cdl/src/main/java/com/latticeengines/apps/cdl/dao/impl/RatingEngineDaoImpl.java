package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.RatingEngineDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;

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
            return super.findAllByFields("status", RatingEngineStatus.valueOf(status));
        } else if (type != null && status == null) {
            return super.findAllByFields("type", RatingEngineType.valueOf(type));
        } else {
            return super.findAllByFields("type", RatingEngineType.valueOf(type), "status",
                    RatingEngineStatus.valueOf(status));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> findAllIdsInSegment(String segmentName) {
        Session session = sessionFactory.getCurrentSession();
        Class<RatingEngine> entityClz = getEntityClass();
        String queryPattern = "select re.id from %s as re";
        if (StringUtils.isNotBlank(segmentName)) {
            queryPattern += " where re.segment.name = :segmentName";
        }
        String queryStr = String.format(queryPattern, entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        System.out.println(query.getQueryString());
        if (StringUtils.isNotBlank(segmentName)) {
            query.setParameter("segmentName", segmentName);
        }
        return (List<String>) query.list();
    }

    @Override
    public void deleteById(String id) {
        super.deleteById(id, true);
    }

}
