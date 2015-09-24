package com.latticeengines.propdata.api.dao.entitlements.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementSourceColumnsPackagesDao;

@Component
public class EntitlementSourceColumnsPackagesDaoImpl
        extends BaseDaoWithAssignedSessionFactoryImpl<EntitlementSourceColumnsPackages>
        implements EntitlementSourceColumnsPackagesDao {

    @Override
    protected Class<EntitlementSourceColumnsPackages> getEntityClass() {
        return EntitlementSourceColumnsPackages.class;
    }

    @Override
    public Class<EntitlementSourceColumnsPackages> getPackageClass() { return EntitlementSourceColumnsPackages.class; }

    @Override
    public EntitlementSourceColumnsPackages getById(Long packageID) {
        return findByKey(EntitlementSourceColumnsPackages.class, packageID);
    }

    @SuppressWarnings("unchecked")
    @Override
    public EntitlementSourceColumnsPackages getByName(String packageName) {
        Session session = sessionFactory.getCurrentSession();
        Class<EntitlementSourceColumnsPackages> entityClz = getEntityClass();
        String queryStr = String.format("from %s where SourceColumnsPackageName = :packageName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("packageName", packageName);
        List<EntitlementSourceColumnsPackages> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<EntitlementSourceColumnsPackages> getByIds(List<Long> ids) {
        Session session = sessionFactory.getCurrentSession();
        Class<EntitlementSourceColumnsPackages> entityClz = getEntityClass();
        if (ids.isEmpty()) { return new ArrayList<>(); }

        String idsStr = "(";
        for (Long id: ids) {
            idsStr += String.valueOf(id) + ",";
        }
        idsStr = idsStr.substring(0, idsStr.length() - 1) + ")";
        String queryStr = String.format("from %s where SourceColumnsPackage_ID IN %s", entityClz.getSimpleName(), idsStr);
        Query query = session.createQuery(queryStr);
        return query.list();
    }

}
