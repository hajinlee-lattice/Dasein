package com.latticeengines.propdata.collection.entitymanager.impl;

import java.io.IOException;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.propdata.collection.entitymanager.PivotProgressEntityMgr;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;


@Component
public class PivotProgressEntityMgrImplTestNG extends PropDataCollectionFunctionalTestNGBase {

    @Autowired
    private PivotProgressEntityMgr progressEntityMgr;

    @Autowired
    @Qualifier(value = "featurePivotedSource")
    PivotedSource source;

    @Test(groups = "functional")
    public void testInsertNew() throws IOException {
        PivotProgress progress =
                progressEntityMgr.insertNewProgress(source, new Date(), "FunctionalTest");
        Assert.assertNotNull(progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID()));
        progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertNull(progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID()));
    }

}
