package com.latticeengines.propdata.api.dao.entitlements.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementContractPackageMap;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementContractPackageMapDao;

@Component("entitlementContractPackageMapDao")
public class EntitlementContractPackageMapDaoImpl
        extends BaseDaoWithAssignedSessionFactoryImpl<EntitlementContractPackageMap>
        implements EntitlementContractPackageMapDao {

    public EntitlementContractPackageMapDaoImpl(){
        super();
    }
    
    @Override
    protected Class<EntitlementContractPackageMap> getEntityClass() {
        return EntitlementContractPackageMap.class;
    }
    
    @SuppressWarnings({ "unchecked" })
    @Override
    public List<EntitlementContractPackageMap> findByPackageId(Long packageId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Package_ID = :packageId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("packageId", packageId);
        return query.list();
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<EntitlementContractPackageMap> findByContractId(String contractID) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Contract_ID = :contractID", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("contractID", contractID);
        return query.list();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EntitlementContractPackageMap findByIds(Long packageID, String externalID) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Package_ID = :packageID "
                + "AND Contract_ID = :externalID", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("packageID", packageID);
        query.setString("externalID", externalID);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (EntitlementContractPackageMap)list.get(0);
    }

}
