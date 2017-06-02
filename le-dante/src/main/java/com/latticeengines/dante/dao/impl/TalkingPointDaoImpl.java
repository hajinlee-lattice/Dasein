package com.latticeengines.dante.dao.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.dao.TalkingPointDao;
import com.latticeengines.dantedb.exposed.dao.impl.BaseDanteDaoImpl;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

@Component("talkingPointDao")
public class TalkingPointDaoImpl extends BaseDanteDaoImpl<DanteTalkingPoint> implements TalkingPointDao {

    private static final Logger log = Logger.getLogger(TalkingPointDaoImpl.class);

    @Override
    protected Class<DanteTalkingPoint> getEntityClass() {
        return DanteTalkingPoint.class;
    }

    @SuppressWarnings("unchecked")
    public List<DanteTalkingPoint> findAllByPlayID(String playID) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("select tp from %s tp " + "where tp.playExternalID = :playExternalID",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("playExternalID", playID);
        return (List<DanteTalkingPoint>) query.list();
    }
}
