package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.dependency.Dependable;
import com.latticeengines.domain.exposed.metadata.DependableObject;
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
    public DependableObject find(String type, String name) {
        DependableObject retrieved = dependableObjectDao.findByFields("type", type, "name", name);
        if (retrieved != null) {
            List<DependableObject> children = getChildren(retrieved);
            if (children != null) {
                retrieved.setDependencies(children);
            }
        }
        return retrieved;
    }

    private List<DependableObject> getChildren(DependableObject parent) {
        HibernateUtils.inflateDetails(parent.getDependencyLinks());
        List<DependableObject> children = new ArrayList<>();
        for (DependencyLink link : parent.getDependencyLinks()) {
            children.add(find(link.getChildType(), link.getChildName()));
        }
        return children;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(DependableObject dependableObject) {
        dependableObject.setTenant(MultiTenantContext.getTenant());
        super.create(dependableObject);

        for (Dependable dependable : dependableObject.getDependencies()) {
            DependencyLink link = new DependencyLink();
            link.setParent(dependableObject);
            link.setChildName(dependable.getName());
            link.setChildType(dependable.getType());
            dependencyLinkDao.create(link);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(String type, String name) {
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
