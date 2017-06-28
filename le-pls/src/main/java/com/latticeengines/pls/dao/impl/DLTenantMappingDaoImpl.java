package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;
import com.latticeengines.pls.dao.DLTenantMappingDao;

@Component("dlTenantMappingDao")
public class DLTenantMappingDaoImpl extends BaseDaoImpl<DLTenantMapping> implements DLTenantMappingDao {
    @Override
    protected Class<DLTenantMapping> getEntityClass() {
        return DLTenantMapping.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public DLTenantMapping getDLTenantMapping(String dlTenantId, String dlLoadGroup) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DLTenantMapping> entityClz = getEntityClass();
        String queryStr = String.format("from %s where DL_TENANT_ID = :dlTenantId and DL_LOAD_GROUP = :dlLoadGroup",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("dlTenantId", dlTenantId);
        query.setString("dlLoadGroup", dlLoadGroup);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (DLTenantMapping)list.get(0);
    }
}
