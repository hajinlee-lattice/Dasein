package com.latticeengines.datacloud.madison.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.MadisonLogicDailyProgress;

public interface MadisonLogicDailyProgressDao extends BaseDao<MadisonLogicDailyProgress> {

    MadisonLogicDailyProgress getNextAvailableDailyProgress();

}
