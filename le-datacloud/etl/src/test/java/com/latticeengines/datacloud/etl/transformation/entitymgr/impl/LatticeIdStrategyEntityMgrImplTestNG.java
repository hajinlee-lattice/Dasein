package com.latticeengines.datacloud.etl.transformation.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.datacloud.etl.transformation.entitymgr.LatticeIdStrategyEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.LatticeIdStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.LatticeIdStrategy.IdType;

@Component("latticeIdStrategyEntityMgrImplTestNG")
public class LatticeIdStrategyEntityMgrImplTestNG extends DataCloudEtlFunctionalTestNGBase {

    @Autowired
    private LatticeIdStrategyEntityMgr latticeIdStrategyEntityMgr;

    private static final String STRATEGY = "AccountMasterSeedRebuild";

    @Test(groups = "functional", enabled = true)
    public void testLatticeIdStrategy() {
        LatticeIdStrategy strategy = latticeIdStrategyEntityMgr.getStrategyByName(STRATEGY);
        Assert.assertNotNull(strategy);
        Assert.assertEquals(strategy.getStrategy(), STRATEGY);
        Assert.assertEquals(strategy.getEntity(), LatticeIdStrategy.Entity.ACCOUNT);
        Assert.assertEquals(strategy.getIdName(), "LatticeID");
        Assert.assertEquals(strategy.getIdType(), IdType.LONG);
        Assert.assertEquals(strategy.getKeyMap().size(), 1);

        Assert.assertEquals(strategy.getKeyMap().get("DOMAIN_DUNS").size(), 2);
        Assert.assertEquals(strategy.getKeyMap().get("DOMAIN_DUNS").get(0), "Domain");
        Assert.assertEquals(strategy.getKeyMap().get("DOMAIN_DUNS").get(1), "DUNS");
    }
}
