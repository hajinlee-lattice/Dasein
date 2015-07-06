package com.latticeengines.propdata.api.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsContractPackageMap;
import com.latticeengines.propdata.api.dao.EntitlementSourceColumnsContractPackageMapDao;

public class EntitlementSourceColumnsContractPackageMapDaoImpl extends
		BaseDaoImpl<EntitlementSourceColumnsContractPackageMap> implements
		EntitlementSourceColumnsContractPackageMapDao {

	public EntitlementSourceColumnsContractPackageMapDaoImpl() {
		super();
	}

	@Override
	protected Class<EntitlementSourceColumnsContractPackageMap> getEntityClass() {
		return EntitlementSourceColumnsContractPackageMap.class;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<EntitlementSourceColumnsContractPackageMap> findByContractID(
			String contract_ID) {
		Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementSourceColumnsContractPackageMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Contract_ID = :contract_ID", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("contract_ID", contract_ID);
		List list = query.list();
        return list;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public EntitlementSourceColumnsContractPackageMap findByContent(Long packageID, String externalID) {
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
