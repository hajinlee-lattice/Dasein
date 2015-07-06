package com.latticeengines.propdata.api.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.DomainFeatureTable;
import com.latticeengines.propdata.api.dao.DomainFeatureTableDao;

public class DomainFeatureTableDaoImpl extends BaseDaoImpl<DomainFeatureTable>
		implements DomainFeatureTableDao {

	@SuppressWarnings("rawtypes")
	@Override
	public DomainFeatureTable findByLookupID(String lookupID) {
		Session session = getSessionFactory().getCurrentSession();
        Class<DomainFeatureTable> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Lookup_ID = :lookupID", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("lookupID", lookupID);
		List list = query.list();
		if (list.size() == 0) {
            return null;
        }
        return (DomainFeatureTable)list.get(0);
	}

	@Override
	protected Class<DomainFeatureTable> getEntityClass() {
		return DomainFeatureTable.class;
	}

}
