package com.latticeengines.datacloud.etl.purge.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.etl.purge.entitymgr.PurgeStrategyEntityMgr;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component
public class PurgeStrategyEntityMgrImplTestNG extends DataCloudEtlFunctionalTestNGBase {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PurgeStrategyEntityMgrImplTestNG.class);

    @Autowired
    private PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    private final static String TEST_SRC1 = PurgeStrategyEntityMgrImplTestNG.class.getSimpleName() + "_TestSrc1";
    private final static String TEST_SRC2 = PurgeStrategyEntityMgrImplTestNG.class.getSimpleName() + "_TestSrc2";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareStrategies();
    }

    @Test(groups = "functional")
    public void testFindStrategiesByType() {
        List<PurgeStrategy> list = purgeStrategyEntityMgr.findStrategiesByType(SourceType.ACCOUNT_MASTER);
        Assert.assertEquals(list.size(), 1);
    }

    @AfterClass(groups = "functional")
    public void destroy() throws Exception {
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategiesBySource(TEST_SRC1));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategiesBySource(TEST_SRC2));
    }

    private void prepareStrategies() {
        List<PurgeStrategy> list = new ArrayList<>();
        PurgeStrategy ps1 = new PurgeStrategy();
        ps1.setSource(TEST_SRC1);
        ps1.setSourceType(SourceType.ACCOUNT_MASTER);
        ps1.setHdfsVersions(2);
        ps1.setS3Days(200);
        ps1.setGlacierDays(1000);
        ps1.setNoBak(false);
        list.add(ps1);
        PurgeStrategy ps2 = new PurgeStrategy();
        ps2.setSource(TEST_SRC2);
        ps2.setSourceType(SourceType.GENERAL_SOURCE);
        ps2.setHdfsDays(30);
        ps2.setS3Days(200);
        ps2.setGlacierDays(200);
        ps2.setNoBak(false);
        list.add(ps2);
        purgeStrategyEntityMgr.insertAll(list);
    }
}
