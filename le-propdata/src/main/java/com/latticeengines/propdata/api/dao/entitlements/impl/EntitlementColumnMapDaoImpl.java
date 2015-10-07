package com.latticeengines.propdata.api.dao.entitlements.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementColumnMap;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementColumnMapDao;

@Component("entitlementColumnMapDao")
public class EntitlementColumnMapDaoImpl extends
        BaseDaoWithAssignedSessionFactoryImpl<EntitlementColumnMap> implements EntitlementColumnMapDao {

    public EntitlementColumnMapDaoImpl() {
        super();
    }

    @Override
    protected Class<EntitlementColumnMap> getEntityClass() {
        return EntitlementColumnMap.class;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<EntitlementColumnMap> getByPackageId(Long packageID){
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementColumnMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Package_ID = :packageID", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("packageID", packageID);
        return query.list();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EntitlementColumnMap findByContent(Long packageID, Long columnCalc) {
        Session session = getSessionFactory().getCurrentSession();
        Class<EntitlementColumnMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Package_ID = :packageID "
                + "AND ColumnCalculation_ID = :columnCalc", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("packageID", packageID);
        query.setLong("columnCalc", columnCalc);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (EntitlementColumnMap)list.get(0);
    }
}
