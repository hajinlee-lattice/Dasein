package com.latticeengines.propdata.api.dao.entitlements.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceContractPackageMap;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementSourceContractPackageMapDao;

@Component
public class EntitlementSourceContractPackageMapDaoImpl
        extends BaseDaoWithAssignedSessionFactoryImpl<EntitlementSourceContractPackageMap>
        implements EntitlementSourceContractPackageMapDao {

    @Override
    protected Class<EntitlementSourceContractPackageMap> getEntityClass() {
        return EntitlementSourceContractPackageMap.class;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<EntitlementSourceContractPackageMap> findByPackageId(Long packageId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourceContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where SourcePackage_ID = :packageId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("packageId", packageId);
        return query.list();
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<EntitlementSourceContractPackageMap> findByContractId(String contractId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourceContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Contract_ID = :contractId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("contractId", contractId);
        return query.list();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EntitlementSourceContractPackageMap findByIds(Long packageID,
            String externalID) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourceContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Contract_ID = :externalID AND SourcePackage_ID = :packageID", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("externalID", externalID);
        query.setLong("packageID", packageID);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (EntitlementSourceContractPackageMap)list.get(0);
    }

}
