package com.latticeengines.db.exposed.entitymgr.impl;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

/**
 * @param <T>
 * @param <ID>
 *
 *            We do not need to add @Transactional annotation in base class.
 *            Because SpringJpaRepository takes care of Transactional
 *            propogations.
 */
public abstract class BaseEntityMgrRepositoryImpl<T extends HasPid, ID> extends BaseEntityMgrImpl<T>
        implements BaseEntityMgrRepository<T, ID> {

    public BaseEntityMgrRepositoryImpl() {
    }

    protected abstract BaseJpaRepository<T, ID> getRepository();

    @Override
    public List<T> findAll() {
        return getRepository().findAll();
    }

    @Override
    public void delete(T entity) {
        if (entity != null && !containInSession(entity)) {
            entity = findByKey(entity);
        }
        super.delete(entity);
    }
}
