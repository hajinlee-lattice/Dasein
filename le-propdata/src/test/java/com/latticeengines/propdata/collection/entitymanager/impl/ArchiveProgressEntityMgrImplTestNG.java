package com.latticeengines.propdata.collection.entitymanager.impl;

import java.io.IOException;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.source.impl.CollectionSource;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;



@Component
public class ArchiveProgressEntityMgrImplTestNG extends PropDataCollectionFunctionalTestNGBase {

    @Autowired
    private ArchiveProgressEntityMgr progressEntityMgr;

    @Test(groups = "functional")
    public void testInsertNew() throws IOException {
        ArchiveProgress progress =
                progressEntityMgr.insertNewProgress(CollectionSource.FEATURE, new Date(), new Date(), "FunctionalTest");
        Assert.assertNotNull(progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID()));
        progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertNull(progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID()));
    }

}
