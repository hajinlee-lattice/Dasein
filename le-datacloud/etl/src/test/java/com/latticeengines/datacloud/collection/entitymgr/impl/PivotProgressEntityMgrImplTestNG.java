package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.io.IOException;
import java.util.Date;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.testframework.DataCloudCollectionFunctionalTestNGBase;
import com.latticeengines.datacloud.core.source.impl.FeaturePivoted;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

@Component
public class PivotProgressEntityMgrImplTestNG extends DataCloudCollectionFunctionalTestNGBase {

    @Inject
    private RefreshProgressEntityMgr progressEntityMgr;

    @Inject
    FeaturePivoted source;

    @Test(groups = "functional")
    public void testInsertNew() throws IOException {
        RefreshProgress progress =
                progressEntityMgr.insertNewProgress(source, new Date(), "FunctionalTest");
        Assert.assertNotNull(progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID()));
        progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertNull(progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID()));
    }

}
