package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.dependency.Dependable;
import com.latticeengines.domain.exposed.metadata.DependableObject;
import com.latticeengines.domain.exposed.metadata.DependableType;
import com.latticeengines.domain.exposed.metadata.DependencyLink;
import com.latticeengines.metadata.dao.DependableObjectDao;
import com.latticeengines.metadata.dao.DependencyLinkDao;
import com.latticeengines.metadata.entitymgr.DependableObjectEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dependableObjectEntityMgr")
public class DependableObjectEntityMgrImpl extends BaseEntityMgrImpl<DependableObject> implements
        DependableObjectEntityMgr {
    @Autowired
    private DependableObjectDao dependableObjectDao;

    @Autowired
    private DependencyLinkDao dependencyLinkDao;

    @Override
    public BaseDao<DependableObject> getDao() {
        return dependableObjectDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @SuppressWarnings("unchecked")
    public DependableObject find(DependableType type, String name) {
        DependableObject retrieved = dependableObjectDao.findByFields("type", Integer.toString(type.ordinal()), "name",
                name);
        if (retrieved != null) {
            HibernateUtils.inflateDetails(retrieved.getDependencyLinks());
            for (DependencyLink link : retrieved.getDependencyLinks()) {
                DependableObject object = new DependableObject();
                object.setName(link.getChildName());
                object.setType(link.getChildType());
                retrieved.addDependency(object);
            }
        }
        return retrieved;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(DependableObject dependableObject) {
        dependableObject.setTenant(MultiTenantContext.getTenant());
        super.create(dependableObject);

        for (Dependable child : dependableObject.getDependencies()) {
            DependencyLink link = new DependencyLink();
            link.setChildType(child.getDependableType());
            link.setChildName(child.getDependableName());
            link.setParent(dependableObject);
            dependencyLinkDao.create(link);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(DependableType type, String name) {
        DependableObject existing = find(type, name);
        if (existing != null) {
            delete(existing);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(DependableObject dependableObject) {
        super.delete(dependableObject);
    }
}
