package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.pls.dao.PlayDao;
import com.latticeengines.pls.entitymanager.PlayEntityMgr;

@Component("playEntityMgr")
public class PlayEntityMgrImpl extends BaseEntityMgrImpl<Play> implements PlayEntityMgr {

    @Autowired
    private PlayDao playDao;

    @Override
    public BaseDao<Play> getDao() {
        return playDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(Play entity) {
        playDao.create(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Play> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Play findByName(String name) {
        return playDao.findByName(name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByName(String name) {
        Play play = findByName(name);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_18144, new String[] { name });
        }
        super.delete(play);
    }

}
