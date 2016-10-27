package com.latticeengines.datacloud.match.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.match.dao.MatchBlockDao;
import com.latticeengines.datacloud.match.entitymgr.MatchBlockEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.MatchBlock;

@Component("matchBlockEntityMgr")
public class MatchBlockEntityMgrImpl implements MatchBlockEntityMgr {


    @Autowired
    private MatchBlockDao matchBlockDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public MatchBlock createBlock(MatchBlock block) {
        matchBlockDao.create(block);
        return matchBlockDao.findByField("BlockOperationUID", block.getBlockOperationUid());
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public MatchBlock updateBlock(MatchBlock block) {
        matchBlockDao.update(block);
        return matchBlockDao.findByField("BlockOperationUID", block.getBlockOperationUid());
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public MatchBlock findByBlockUid(String blockUid) {
        return matchBlockDao.findByField("BlockOperationUID", blockUid);
    }

}
