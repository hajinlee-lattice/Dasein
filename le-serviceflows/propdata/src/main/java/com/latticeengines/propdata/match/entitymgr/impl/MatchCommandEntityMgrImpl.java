package com.latticeengines.propdata.match.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.propdata.match.dao.MatchCommandDao;
import com.latticeengines.propdata.match.entitymgr.MatchCommandEntityMgr;

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
}
