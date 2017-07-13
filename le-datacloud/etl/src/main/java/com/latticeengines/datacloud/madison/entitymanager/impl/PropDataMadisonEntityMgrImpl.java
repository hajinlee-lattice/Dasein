package com.latticeengines.datacloud.madison.entitymanager.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.datacloud.MadisonLogicDailyProgress;

@Component("propDataEntityMgr")
public class PropDataMadisonEntityMgrImpl implements com.latticeengines.datacloud.madison.entitymanager.PropDataMadisonEntityMgr {

    @SuppressWarnings("unused")
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private com.latticeengines.datacloud.madison.dao.MadisonLogicDailyProgressDao dailyProgressDao;

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
