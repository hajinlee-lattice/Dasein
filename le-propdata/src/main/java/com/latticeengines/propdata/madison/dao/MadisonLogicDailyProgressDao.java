package com.latticeengines.propdata.madison.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.MadisonLogicDailyProgress;

public interface MadisonLogicDailyProgressDao extends BaseDao<MadisonLogicDailyProgress> {

    MadisonLogicDailyProgress getNextAvailableDailyProgress();

}
