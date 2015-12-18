package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.TargetMarket;

public interface TargetMarketEntityMgr extends BaseEntityMgr<TargetMarket> {

    void deleteTargetMarketByName(String name);

    TargetMarket findTargetMarketByName(String name);

    List<TargetMarket> findAllTargetMarkets();
    
    void updateTargetMarketByName(TargetMarket targetMarket, String name);

    TargetMarket createDefaultTargetMarket();
}
