package com.latticeengines.datacloud.madison.entitymanager;

import com.latticeengines.domain.exposed.datacloud.MadisonLogicDailyProgress;

public interface PropDataMadisonEntityMgr {

    MadisonLogicDailyProgress getNextAvailableDailyProgress();


    void executeUpdate(MadisonLogicDailyProgress dailyProgress);


    void create(MadisonLogicDailyProgress dailyProgress);


    void delete(MadisonLogicDailyProgress dailyProgress);


    MadisonLogicDailyProgress findByKey(MadisonLogicDailyProgress dailyProgress);

}
