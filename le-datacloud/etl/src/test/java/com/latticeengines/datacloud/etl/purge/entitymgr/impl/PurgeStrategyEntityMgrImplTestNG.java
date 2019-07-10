package com.latticeengines.datacloud.etl.purge.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

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

    @Inject
    private PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    private static final String TEST_SRC1 = PurgeStrategyEntityMgrImplTestNG.class.getSimpleName() + "_TestSrc1";
    private static final String TEST_SRC2 = PurgeStrategyEntityMgrImplTestNG.class.getSimpleName() + "_TestSrc2";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareStrategies();
    }

    @Test(groups = "functional")
    public void testFindStrategiesByType() {
        List<PurgeStrategy> list = purgeStrategyEntityMgr.findStrategiesByType(SourceType.AM_SOURCE);
        Assert.assertEquals(list.size(), 1);
    }

    @Test(groups = "functional")
    public void testFindStrategiesBySourceAndType() {
        PurgeStrategy ps1 = purgeStrategyEntityMgr.findStrategyBySourceAndType(TEST_SRC1, SourceType.AM_SOURCE);
        PurgeStrategy ps2 = purgeStrategyEntityMgr.findStrategyBySourceAndType(TEST_SRC2, SourceType.GENERAL_SOURCE);
        PurgeStrategy ps3 = purgeStrategyEntityMgr.findStrategyBySourceAndType(TEST_SRC2, SourceType.AM_SOURCE);
        Assert.assertNotNull(ps1);
        Assert.assertNotNull(ps2);
        Assert.assertNull(ps3);
    }

    @AfterClass(groups = "functional")
    public void destroy() throws Exception {
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategyBySource(TEST_SRC1));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategyBySource(TEST_SRC2));
    }

    private void prepareStrategies() {
        List<PurgeStrategy> list = new ArrayList<>();
        PurgeStrategy ps1 = new PurgeStrategy();
        ps1.setSource(TEST_SRC1);
        ps1.setSourceType(SourceType.AM_SOURCE);
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
