package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.io.IOException;
import java.util.Date;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.testframework.DataCloudCollectionFunctionalTestNGBase;
import com.latticeengines.datacloud.core.source.impl.Feature;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;

@Component
public class ArchiveProgressEntityMgrImplTestNG extends DataCloudCollectionFunctionalTestNGBase {

    @Inject
    private ArchiveProgressEntityMgr progressEntityMgr;

    @Inject
    Feature source;

    @Test(groups = "functional")
    public void testInsertNew() throws IOException {
        ArchiveProgress progress =
                progressEntityMgr.insertNewProgress(source, new Date(), new Date(), "FunctionalTest");
        Assert.assertNotNull(progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID()));
        progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertNull(progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID()));
    }

}
