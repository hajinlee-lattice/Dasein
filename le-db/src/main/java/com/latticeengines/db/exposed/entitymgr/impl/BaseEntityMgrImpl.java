package com.latticeengines.db.exposed.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.graph.EdgeType;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.security.Tenant;

public abstract class BaseEntityMgrImpl<T extends HasPid> implements BaseEntityMgr<T> {

    public BaseEntityMgrImpl() {
    }

    public abstract BaseDao<T> getDao();

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void create(T entity) {
        getDao().create(entity);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(T entity) {
        getDao().createOrUpdate(entity);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void update(T entity) {
        getDao().update(entity);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void delete(T entity) {
        getDao().delete(entity);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void deleteAll() {
        getDao().deleteAll();
    }

    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    @Override
    public boolean containInSession(T entity) {
        return getDao().containInSession(entity);
    }

    /**
     * get object by key. entity.getPid() must NOT be empty.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public T findByKey(T entity) {
        return getDao().findByKey(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public T findByField(String fieldName, Object value) {
        return getDao().findByField(fieldName, value);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<T> findAll() {
        return getDao().findAll();
    }

    @Override
    public ParsedDependencies parse(T entity, T existingEntity) {
        Tenant tenant = MultiTenantContext.getTenant();

        ParsedDependencies parsedDependencies = new ParsedDependencies();
        List<Triple<String, String, String>> addDependencies = new ArrayList<>();
        List<Triple<String, String, String>> removeDependencies = new ArrayList<>();
        if (existingEntity == null) {
            if (!shouldSkipTenantDependency()) {
                addDependencies.add(ParsedDependencies //
                        .tuple(tenant.getId(), VertexType.TENANT, EdgeType.TENANT));
            }
            Set<Triple<String, String, String>> attrDepSet = extractDependencies(entity);
            if (CollectionUtils.isNotEmpty(attrDepSet)) {
                addDependencies.addAll(attrDepSet);
            }
        } else {
            Set<Triple<String, String, String>> attrDepSet = extractDependencies(entity);
            Set<Triple<String, String, String>> existingAttrDepSet = extractDependencies(existingEntity);
            if (CollectionUtils.isNotEmpty(attrDepSet)) {
                if (CollectionUtils.isNotEmpty(existingAttrDepSet)) {
                    attrDepSet.stream().filter(a -> !existingAttrDepSet.contains(a))
                            .forEach(a -> addDependencies.add(a));
                } else {
                    addDependencies.addAll(attrDepSet);
                }
            }
            if (CollectionUtils.isNotEmpty(existingAttrDepSet)) {
                if (CollectionUtils.isNotEmpty(attrDepSet)) {
                    existingAttrDepSet.stream().filter(a -> !attrDepSet.contains(a))
                            .forEach(a -> removeDependencies.add(a));
                } else {
                    removeDependencies.addAll(existingAttrDepSet);
                }
            }
        }
        parsedDependencies.setAddDependencies(addDependencies);
        parsedDependencies.setRemoveDependencies(removeDependencies);
        return parsedDependencies;
    }

    public Set<Triple<String, String, String>> extractDependencies(T entity) {
        return null;
    }

    public boolean shouldSkipTenantDependency() {
        return false;
    }
}
