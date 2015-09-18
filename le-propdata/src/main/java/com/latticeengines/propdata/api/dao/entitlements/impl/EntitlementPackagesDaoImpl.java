package com.latticeengines.propdata.api.dao.entitlements.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementPackagesDao;

@Component("entitlementPackagesDao")
public class EntitlementPackagesDaoImpl extends BaseDaoImpl<EntitlementPackages> implements EntitlementPackagesDao {

    @Override
    protected Class<EntitlementPackages> getEntityClass() {
        return EntitlementPackages.class;
    }

    @Override
    public Class<EntitlementPackages> getPackageClass() { return EntitlementPackages.class; }

    @Override
    public EntitlementPackages getById(Long packageID) {
        return findByKey(EntitlementPackages.class, packageID);
    }

    @SuppressWarnings("unchecked")
    @Override
    public EntitlementPackages getByName(String packageName) {
        Session session = sessionFactory.getCurrentSession();
        Class<EntitlementPackages> entityClz = getEntityClass();
        String queryStr = String.format("from %s where PackageName = :packageName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("packageName", packageName);
        List<EntitlementPackages> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<EntitlementPackages> getByIds(List<Long> ids) {
        Session session = sessionFactory.getCurrentSession();
        Class<EntitlementPackages> entityClz = getEntityClass();
        if (ids.isEmpty()) { return new ArrayList<>(); }

        String idsStr = "(";
        for (Long id: ids) {
            idsStr += String.valueOf(id) + ",";
        }
        idsStr = idsStr.substring(0, idsStr.length() - 1) + ")";
        String queryStr = String.format("from %s where Package_ID IN %s", entityClz.getSimpleName(), idsStr);
        Query query = session.createQuery(queryStr);
        return query.list();
    }
}
