package com.latticeengines.propdata.collection.entitymanager.impl;

import java.io.IOException;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.source.impl.FeaturePivoted;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;


@Component
public class PivotProgressEntityMgrImplTestNG extends PropDataCollectionFunctionalTestNGBase {

    @Autowired
    private RefreshProgressEntityMgr progressEntityMgr;

    @Autowired
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
