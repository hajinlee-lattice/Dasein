package com.latticeengines.propdata.api.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.propdata.api.dao.EntitlementSourceColumnsPackageMapDao;

public class EntitlementSourceColumnsPackageMapDaoImpl extends
        BaseDaoImpl<EntitlementSourceColumnsPackageMap> implements
        EntitlementSourceColumnsPackageMapDao {

    public EntitlementSourceColumnsPackageMapDaoImpl() {
        super();
    }

    @Override
    protected Class<EntitlementSourceColumnsPackageMap> getEntityClass() {
        return EntitlementSourceColumnsPackageMap.class;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public List<EntitlementSourceColumnsPackageMap> findByPackageID(
            Long packageID) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourceColumnsPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where SourceColumnsPackage_ID = :packageID"
                , entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("packageID", packageID);
        List list = query.list();
        return list;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EntitlementSourceColumnsPackageMap findByContent(Long packageID,
            String lookupID, String columnName) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourceColumnsPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where SourceColumnsPackage_ID = :packageID"
                + " AND Lookup_ID = :lookupID AND ColumnName = :columnName"
                , entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("packageID", packageID);
        query.setString("lookupID", lookupID);
        query.setString("columnName", columnName);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (EntitlementSourceColumnsPackageMap)list.get(0);
    }

}
