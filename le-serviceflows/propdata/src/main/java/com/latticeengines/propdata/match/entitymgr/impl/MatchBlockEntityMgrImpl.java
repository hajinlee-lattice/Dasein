package com.latticeengines.propdata.match.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.MatchBlock;
import com.latticeengines.propdata.match.dao.MatchBlockDao;
import com.latticeengines.propdata.match.entitymgr.MatchBlockEntityMgr;

@Component("matchBlockEntityMgr")
public class MatchBlockEntityMgrImpl implements MatchBlockEntityMgr {


    @Autowired
    private MatchBlockDao matchBlockDao;

    @Override
    @Transactional(value = "propDataManage")
    public MatchBlock createBlock(MatchBlock block) {
        matchBlockDao.create(block);
        return matchBlockDao.findByField("BlockOperationUID", block.getBlockOperationUid());
    }

    @Override
    @Transactional(value = "propDataManage")
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
