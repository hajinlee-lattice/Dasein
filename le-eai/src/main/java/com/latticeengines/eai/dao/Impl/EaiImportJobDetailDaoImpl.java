package com.latticeengines.eai.dao.Impl;

import java.util.List;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.eai.dao.EaiImportJobDetailDao;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

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
        query.setString("identifier", identifier);
        query.setMaxResults(1);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (EaiImportJobDetail)list.get(0);
        }
    }
}
