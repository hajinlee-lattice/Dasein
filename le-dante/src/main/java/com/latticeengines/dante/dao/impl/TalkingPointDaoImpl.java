package com.latticeengines.dante.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.dao.TalkingPointDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.TalkingPoint;

@Component("talkingPointDao")
public class TalkingPointDaoImpl extends BaseDaoImpl<TalkingPoint> implements TalkingPointDao {
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
        Query query = session.createQuery(queryStr);
        query.setParameter("playName", playName);
        return (List<TalkingPoint>) query.list();
    }

}
