package com.latticeengines.datacloud.collection.service.impl;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.collection.testframework.DataCloudCollectionFunctionalTestNGBase;
import com.latticeengines.datacloud.core.source.BulkSource;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

//DataCloud SQL Servers are shutdown. Disable related tests.
@Deprecated
public abstract class RefreshBulkServiceImplTestNGBase extends DataCloudCollectionFunctionalTestNGBase {
    RefreshService refreshService;
    RefreshProgressEntityMgr progressEntityMgr;
    DerivedSource source;
    BulkSource baseSource;
    Collection<RefreshProgress> progresses = new HashSet<>();

    String baseSourceVersion;

    abstract RefreshService getRefreshService();

    abstract RefreshProgressEntityMgr getProgressEntityMgr();

    abstract DerivedSource getSource();

    abstract BulkArchiveServiceImplTestNGBase getBaseSourceTestBean();

    @BeforeMethod(groups = "collection", enabled = false)
    public void setUp() throws Exception {
        source = getSource();
        prepareCleanPod(source);
        getBaseSourceTestBean().setupBeans();
        refreshService = getRefreshService();
        progressEntityMgr = getProgressEntityMgr();
        baseSource = (BulkSource) source.getBaseSources()[0];
    }

    @AfterMethod(groups = "collection", enabled = false)
    public void tearDown() throws Exception {
        getBaseSourceTestBean().tearDown();
    }

    @Test(groups = "collection", enabled = false)
    public void testWholeProgress() {
        ArchiveProgress archiveProgress;
        RefreshProgress progress;

        getBaseSourceTestBean().purgeRawData();
        archiveProgress = getBaseSourceTestBean().createNewProgress();
        getBaseSourceTestBean().importFromDB(archiveProgress);
        getBaseSourceTestBean().finish(archiveProgress);

        progress = createNewProgress(new Date());
        progress = transformData(progress);
        progress = exportToDB(progress);
        finish(progress);

        verifyResultTable();
        cleanupProgressTables();
    }

    protected RefreshProgress createNewProgress(Date pivotDate) {
        baseSourceVersion = hdfsSourceEntityMgr.getCurrentVersion(baseSource);
        RefreshProgress progress = refreshService.startNewProgress(pivotDate, baseSourceVersion, progressCreator);
        Assert.assertNotNull(progress, "Should have a progress in the job context.");
        Long pid = progress.getPid();
        Assert.assertNotNull(pid, "The new progress should have a pid assigned.");
        progresses.add(progress);
        return progress;
    }

    protected RefreshProgress transformData(RefreshProgress progress) {
        RefreshProgress response = refreshService.transform(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.TRANSFORMED);

        RefreshProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.TRANSFORMED);

        return response;
    }

    protected RefreshProgress exportToDB(RefreshProgress progress) {
        RefreshProgress response = refreshService.exportToDB(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.UPLOADED);

        RefreshProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.UPLOADED);

        return response;
    }

    protected RefreshProgress finish(RefreshProgress progress) {
        RefreshProgress response = refreshService.finish(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.FINISHED);

        RefreshProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.FINISHED);

        return response;
    }

    protected void cleanupProgressTables() {
        for (RefreshProgress progress : progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
        getBaseSourceTestBean().cleanupProgressTables();
    }

    protected void verifyResultTable() {
    }
}
