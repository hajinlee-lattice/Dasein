package com.latticeengines.modelquality.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.dao.PropDataDao;

@Component("propDataDao")
public class PropDataDaoImpl extends ModelQualityBaseDaoImpl<PropData> implements PropDataDao {

    @Override
    protected Class<PropData> getEntityClass() {
        return PropData.class;
    }

    @Override
    public PropData findByMaxVersion() {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where version = (select MAX(version) from %s)",
                getEntityClass().getSimpleName(), getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        @SuppressWarnings("unchecked")
        List<PropData> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        if (results.size() > 1) {
            throw new RuntimeException("Multiple rows found with the same value for VERSION");
        }
        return results.get(0);
    }
}
