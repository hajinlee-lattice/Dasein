package com.latticeengines.propdata.api.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.propdata.api.dao.EntitlementSourcePackageMapDao;

public class EntitlementSourcePackageMapDaoImpl extends
        BaseDaoImpl<EntitlementSourcePackageMap> implements
        EntitlementSourcePackageMapDao {

    public EntitlementSourcePackageMapDaoImpl() {
        super();
    }

    @Override
    protected Class<EntitlementSourcePackageMap> getEntityClass() {
        return EntitlementSourcePackageMap.class;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public List<EntitlementSourcePackageMap> findByPackageID(Long packageID) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourcePackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where SourcePackage_ID = :packageID", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("packageID", packageID);
        List list = query.list();
        return list;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EntitlementSourcePackageMap findByContent(Long packageID,
            String lookupID) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourcePackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where SourcePackage_ID = :packageID"
                + " AND Lookup_ID = :lookupID", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("packageID", packageID);
        query.setString("lookupID", lookupID);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (EntitlementSourcePackageMap)list.get(0);
    }

}
