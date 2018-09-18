package com.latticeengines.eai.dao.Impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.eai.dao.EaiImportJobDetailDao;

@Component("eaiImportJobDetailDao")
public class EaiImportJobDetailDaoImpl extends BaseDaoImpl<EaiImportJobDetail> implements EaiImportJobDetailDao {
    @Override
    protected Class<EaiImportJobDetail> getEntityClass() {
        return EaiImportJobDetail.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EaiImportJobDetail findMostRecentRecordByIdentifier(String identifier) {
        Session session = sessionFactory.getCurrentSession();
        Class<EaiImportJobDetail> entityClz = getEntityClass();
        String queryStr = String
                .format(
                        "from %s where COLLECTION_IDENTIFIER = :identifier order by SEQUENCE_ID desc",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("identifier", identifier);
        query.setMaxResults(1);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (EaiImportJobDetail)list.get(0);
        }
    }
}
