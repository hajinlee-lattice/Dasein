package com.latticeengines.datacloud.etl.transformation.entitymgr;

import com.latticeengines.domain.exposed.datacloud.manage.LatticeIdStrategy;

public interface LatticeIdStrategyEntityMgr {
    LatticeIdStrategy getStrategyByName(String strategy);
}
