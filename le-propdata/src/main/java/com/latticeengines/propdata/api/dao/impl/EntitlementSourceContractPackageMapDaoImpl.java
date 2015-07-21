package com.latticeengines.propdata.api.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceContractPackageMap;
import com.latticeengines.propdata.api.dao.EntitlementSourceContractPackageMapDao;

public class EntitlementSourceContractPackageMapDaoImpl extends
        BaseDaoImpl<EntitlementSourceContractPackageMap> implements
        EntitlementSourceContractPackageMapDao {

    @Override
    protected Class<EntitlementSourceContractPackageMap> getEntityClass() {
        return EntitlementSourceContractPackageMap.class;
    }

    public EntitlementSourceContractPackageMapDaoImpl() {
        super();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public List<EntitlementSourceContractPackageMap> findByContractID(
            String contract_ID) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourceContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Contract_ID = :contract_ID", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("contract_ID", contract_ID);
        List list = query.list();
        return list;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EntitlementSourceContractPackageMap findByContent(Long packageID,
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
