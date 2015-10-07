package com.latticeengines.propdata.api.dao.entitlements.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsContractPackageMap;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementSourceColumnsContractPackageMapDao;

@Component
public class EntitlementSourceColumnsContractPackageMapDaoImpl
        extends BaseDaoWithAssignedSessionFactoryImpl<EntitlementSourceColumnsContractPackageMap>
        implements EntitlementSourceColumnsContractPackageMapDao {

    @Override
    protected Class<EntitlementSourceColumnsContractPackageMap> getEntityClass() {
        return EntitlementSourceColumnsContractPackageMap.class;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<EntitlementSourceColumnsContractPackageMap> findByPackageId(Long packageId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourceColumnsContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where SourceColumnsPackage_ID = :packageId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("packageId", packageId);
        return query.list();
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<EntitlementSourceColumnsContractPackageMap> findByContractId(String contractId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourceColumnsContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Contract_ID = :contractId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("contractId", contractId);
        return query.list();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EntitlementSourceColumnsContractPackageMap findByIds(Long packageID, String externalID) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourceColumnsContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Contract_ID = :externalID"
                + " AND SourceColumnsPackage_ID = :packageID", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("externalID", externalID);
        query.setLong("packageID", packageID);
        List list = query.list();
        if(list.size() == 0){
            return null;
        }
        return (EntitlementSourceColumnsContractPackageMap)list.get(0);
    }

}
