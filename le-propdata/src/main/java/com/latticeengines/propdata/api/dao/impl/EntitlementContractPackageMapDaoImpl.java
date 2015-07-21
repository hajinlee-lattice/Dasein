package com.latticeengines.propdata.api.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementContractPackageMap;
import com.latticeengines.propdata.api.dao.EntitlementContractPackageMapDao;

public class EntitlementContractPackageMapDaoImpl extends BaseDaoImpl<EntitlementContractPackageMap> implements
        EntitlementContractPackageMapDao {

    public EntitlementContractPackageMapDaoImpl(){
        super();
    }
    
    @Override
    protected Class<EntitlementContractPackageMap> getEntityClass() {
        return EntitlementContractPackageMap.class;
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public List<EntitlementContractPackageMap> findByContractID(String contractID) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Contract_ID = :contractID", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("contractID", contractID);
        List list = query.list();
        return list;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EntitlementContractPackageMap findByContent(Long packageID,
            String externalID) {
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
