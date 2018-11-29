package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.PublishedTalkingPointDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.PublishedTalkingPoint;

@Component("publishedTalkingPointDao")
public class PublishedTalkingPointDaoImpl extends BaseDaoImpl<PublishedTalkingPoint>
        implements PublishedTalkingPointDao {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PublishedTalkingPointDaoImpl.class);

    @Override
    protected Class<PublishedTalkingPoint> getEntityClass() {
        return PublishedTalkingPoint.class;
    }

    @SuppressWarnings("unchecked")
    public List<PublishedTalkingPoint> findAllByPlayName(String playName) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("select tp from %s tp " + "where tp.playName = :playName",
                getEntityClass().getSimpleName());
        Query<PublishedTalkingPoint> query = session.createQuery(queryStr);
        query.setParameter("playName", playName);
        return (List<PublishedTalkingPoint>) query.list();
    }
}
