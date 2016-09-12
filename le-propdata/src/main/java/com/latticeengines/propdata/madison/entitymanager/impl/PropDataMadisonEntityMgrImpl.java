package com.latticeengines.propdata.madison.entitymanager.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.datacloud.MadisonLogicDailyProgress;
import com.latticeengines.propdata.madison.dao.MadisonLogicDailyProgressDao;
import com.latticeengines.propdata.madison.entitymanager.PropDataMadisonEntityMgr;

@Component("propDataEntityMgr")
public class PropDataMadisonEntityMgrImpl implements PropDataMadisonEntityMgr {

    @SuppressWarnings("unused")
    private final Log log = LogFactory.getLog(this.getClass());

    @Autowired
    private MadisonLogicDailyProgressDao dailyProgressDao;

    @Override
    @Transactional(value = "propdataMadison", readOnly = true)
    public MadisonLogicDailyProgress getNextAvailableDailyProgress() {
        return dailyProgressDao.getNextAvailableDailyProgress();
    }

    @Override
    @Transactional(value = "propdataMadison")
    public void executeUpdate(MadisonLogicDailyProgress dailyProgress) {
        dailyProgressDao.update(dailyProgress);
    }

    @Override
    @Transactional(value = "propdataMadison")
    public void create(MadisonLogicDailyProgress dailyProgress) {
        dailyProgressDao.create(dailyProgress);
    }

    @Override
    @Transactional(value = "propdataMadison")
    public void delete(MadisonLogicDailyProgress dailyProgress) {
        dailyProgressDao.delete(dailyProgress);
    }

    @Override
    @Transactional(value = "propdataMadison")
    public MadisonLogicDailyProgress findByKey(MadisonLogicDailyProgress dailyProgress) {
        return dailyProgressDao.findByKey(dailyProgress);
    }
    
}
