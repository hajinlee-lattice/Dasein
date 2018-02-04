package com.latticeengines.db.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.dao.KeyValueDao;
import com.latticeengines.db.exposed.entitymgr.KeyValueEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.repository.KeyValueRepository;
import com.latticeengines.domain.exposed.workflow.KeyValue;

@Component("keyValueEntityMgr")
public class KeyValueEntityMgrImpl extends BaseEntityMgrRepositoryImpl<KeyValue, Long> implements KeyValueEntityMgr {

    private final KeyValueRepository keyValueRepository;

    private final KeyValueDao keyValueDao;

    @Inject
    public KeyValueEntityMgrImpl(KeyValueRepository keyValueRepository, KeyValueDao keyValueDao) {
        this.keyValueRepository = keyValueRepository;
        this.keyValueDao = keyValueDao;
    }

    @Override
    public BaseDao<KeyValue> getDao() {
        return keyValueDao;
    }

    @Override
    public BaseJpaRepository<KeyValue, Long> getRepository() {
        return keyValueRepository;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<KeyValue> findByTenantId(long tenantId) {
        return keyValueRepository.findByTenantId(tenantId);
    }
}
