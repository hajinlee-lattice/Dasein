package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.TalkingPointDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.TalkingPoint;

@Component("talkingPointDao")
public class TalkingPointDaoImpl extends BaseDaoImpl<TalkingPoint> implements TalkingPointDao {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TalkingPointDaoImpl.class);

    @Override
    protected Class<TalkingPoint> getEntityClass() {
        return TalkingPoint.class;
    }

    @SuppressWarnings("unchecked")
    public List<TalkingPoint> findAllByPlayName(String playName) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("select tp from %s tp " + "where tp.play.name = :playName",
                getEntityClass().getSimpleName());
        Query<TalkingPoint> query = session.createQuery(queryStr);
        query.setParameter("playName", playName);
        return (List<TalkingPoint>) query.list();
    }

}
