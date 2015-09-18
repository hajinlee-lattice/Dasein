package com.latticeengines.propdata.api.dao.entitlements.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementSourcePackagesDao;

@Component
public class EntitlementSourcePackagesDaoImpl extends
        BaseDaoImpl<EntitlementSourcePackages> implements
        EntitlementSourcePackagesDao {

    @Override
    protected Class<EntitlementSourcePackages> getEntityClass() {
        return EntitlementSourcePackages.class;
    }

    @Override
    public Class<EntitlementSourcePackages> getPackageClass() { return EntitlementSourcePackages.class; }

    @Override
    public EntitlementSourcePackages getById(Long packageID) {
        return findByKey(EntitlementSourcePackages.class, packageID);
    }

    @SuppressWarnings("unchecked")
    @Override
    public EntitlementSourcePackages getByName(String packageName) {
        Session session = sessionFactory.getCurrentSession();
        Class<EntitlementSourcePackages> entityClz = getEntityClass();
        String queryStr = String.format("from %s where SourcePackageName = :packageName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("packageName", packageName);
        List<EntitlementSourcePackages> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<EntitlementSourcePackages> getByIds(List<Long> ids) {
        Session session = sessionFactory.getCurrentSession();
        Class<EntitlementSourcePackages> entityClz = getEntityClass();
        if (ids.isEmpty()) { return new ArrayList<>(); }

        String idsStr = "(";
        for (Long id: ids) {
            idsStr += String.valueOf(id) + ",";
        }
        idsStr = idsStr.substring(0, idsStr.length() - 1) + ")";
        String queryStr = String.format("from %s where SourcePackage_ID IN %s", entityClz.getSimpleName(), idsStr);
        Query query = session.createQuery(queryStr);
        return query.list();
    }
}
