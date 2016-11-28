package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.match.dao.MatchCommandDao;
import com.latticeengines.datacloud.match.entitymgr.MatchCommandEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;

@Component("matchCommandEntityMgr")
public class MatchCommandEntityMgrImpl implements MatchCommandEntityMgr {

    @Autowired
    private MatchCommandDao matchCommandDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public MatchCommand createCommand(MatchCommand command) {
        matchCommandDao.create(command);
        return matchCommandDao.findByField("RootOperationUID", command.getRootOperationUid());
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public MatchCommand updateCommand(MatchCommand command) {
        matchCommandDao.update(command);
        return matchCommandDao.findByField("RootOperationUID", command.getRootOperationUid());
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public MatchCommand findByRootOperationUid(String rootUid) {
        return matchCommandDao.findByField("RootOperationUID", rootUid);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public List<MatchCommand> findOutDatedCommands(int retentionDays) {
        return matchCommandDao.findOutDatedCommands(retentionDays);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public void deleteCommand(MatchCommand command) {
        matchCommandDao.deleteCommand(command);

    }
}
