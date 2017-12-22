package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.pls.dao.ActionDao;
import com.latticeengines.pls.entitymanager.ActionEntityMgr;
import com.latticeengines.pls.repository.ActionRepository;

@Component("actionEntityMgr")
public class ActionEntityMgrImpl extends BaseEntityMgrRepositoryImpl<Action, Long> implements ActionEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(ActionEntityMgrImpl.class);

    @Inject
    private ActionRepository actionRepository;

    @Inject
    private ActionDao actionDao;

    @Override
    public BaseJpaRepository<Action, Long> getRepository() {
        return actionRepository;
    }

    @Override
    public BaseDao<Action> getDao() {
        return actionDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Action> findAll() {
        return actionDao.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Action> findByOwnerId(@Nullable Long ownerId, Pageable pageable) {
        if (pageable == null) {
            // TODO change back to use JPA repository after PLS-6214 is done
            // return actionRepository.findByOwnerId(ownerId);
            if (ownerId != null) {
                return actionDao.findAllByField("OWNER_ID", ownerId);
            } else {
                return actionDao.findAllWithNullOwnerId();
            }
        }
        return actionRepository.findByOwnerId(ownerId, pageable);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Action findByPid(Long pid) {
        // TODO change back to use JPA repository after PLS-6214 is done
        // return actionRepository.findByPid(pid);
        return actionDao.findByField("pid", pid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(Action action) {
        super.delete(action);
    }

}
