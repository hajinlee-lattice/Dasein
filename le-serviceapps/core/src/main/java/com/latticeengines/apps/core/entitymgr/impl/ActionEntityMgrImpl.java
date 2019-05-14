package com.latticeengines.apps.core.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.core.dao.ActionDao;
import com.latticeengines.apps.core.entitymgr.ActionEntityMgr;
import com.latticeengines.apps.core.repository.writer.ActionRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;

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
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void create(List<Action> actions) {
        actionDao.create(actions);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void copy(List<Action> actions) {
        actionDao.create(actions, false);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Action> findAll() {
        return actionRepository.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Action> findByOwnerId(@Nullable Long ownerId, Pageable pageable) {
        if (pageable == null) {
            return actionRepository.findByOwnerId(ownerId);
        }
        return actionRepository.findByOwnerId(ownerId, pageable);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Action findByPid(Long pid) {
        return actionRepository.findByPid(pid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(Action action) {
        super.delete(action);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateOwnerIdIn(Long ownerId, List<Long> actionPids) {
        actionDao.updateOwnerIdIn(ownerId, actionPids);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Action> findByPidIn(List<Long> actionPids) {
        return actionRepository.findByPidIn(actionPids);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void cancel(Long actionPid) {
        actionDao.cancel(actionPid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Action> getActionsByJobPids(List<Long> jobPids) {
        return actionRepository.findAllByTrackingPidIn(jobPids);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Action> findByOwnerIdAndActionStatus(Long ownerId, ActionStatus actionStatus) {
        return actionRepository.findByOwnerIdAndActionStatus(ownerId, actionStatus);
    }
}
