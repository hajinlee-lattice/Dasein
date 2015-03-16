package com.latticeengines.madison.entitymanager;

import com.latticeengines.domain.exposed.propdata.MadisonLogicDailyProgress;

public interface PropDataMadisonEntityMgr {

    MadisonLogicDailyProgress getNextAvailableDailyProgress();


    void executeUpdate(MadisonLogicDailyProgress dailyProgress);


    void create(MadisonLogicDailyProgress dailyProgress);


    void delete(MadisonLogicDailyProgress dailyProgress);


    MadisonLogicDailyProgress findByKey(MadisonLogicDailyProgress dailyProgress);

}
