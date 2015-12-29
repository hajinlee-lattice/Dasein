package com.latticeengines.propdata.collection.service.impl;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.collection.source.BulkSource;
import com.latticeengines.propdata.collection.source.ServingSource;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionDeploymentTestNGBase;

abstract public class RefreshBulkServiceImplDeploymentTestNGBase extends PropDataCollectionDeploymentTestNGBase {
    RefreshService refreshService;
    RefreshProgressEntityMgr progressEntityMgr;
    ServingSource source;
    BulkSource baseSource;
    Collection<RefreshProgress> progresses = new HashSet<>();

    String baseSourceVersion;
    abstract RefreshService getRefreshService();
    abstract RefreshProgressEntityMgr getProgressEntityMgr();
    abstract ServingSource getSource();
    abstract BulkArchiveServiceImplDeploymentTestNGBase getBaseSourceTestBean();

    @BeforeMethod(groups = "deployment")
    public void setUp() throws Exception {
        hdfsPathBuilder.changeHdfsPodId("DeploymentTest");
        getBaseSourceTestBean().setUpPod("DeploymentTest");
        refreshService = getRefreshService();
        progressEntityMgr = getProgressEntityMgr();
        source = getSource();
        baseSource = (BulkSource) source.getBaseSource();
    }

    @AfterMethod(groups = "deployment")
    public void tearDown() throws Exception {
        getBaseSourceTestBean().tearDown();
    }

    @Test(groups = "deployment")
    public void testWholeProgress() {
        ArchiveProgress archiveProgress;
        RefreshProgress progress;

        truncateDestTable();

        getBaseSourceTestBean().purgeRawData();
        archiveProgress = getBaseSourceTestBean().createNewProgress();
        getBaseSourceTestBean().importFromDB(archiveProgress);
        getBaseSourceTestBean().finish(archiveProgress);

        progress = createNewProgress(new Date());
        progress = pivotData(progress);
        progress = exportToDB(progress);
        finish(progress);

        verifyResultTable();
        cleanupProgressTables();
    }

    private void truncateDestTable() {
        String tableName = source.getSqlTableName();
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) TRUNCATE TABLE " + tableName);
    }

    protected RefreshProgress createNewProgress(Date pivotDate) {
        baseSourceVersion = hdfsSourceEntityMgr.getCurrentVersion(getSource().getBaseSource());
        RefreshProgress progress = refreshService.startNewProgress(pivotDate, baseSourceVersion, progressCreator);
        Assert.assertNotNull(progress, "Should have a progress in the job context.");
        Long pid = progress.getPid();
        Assert.assertNotNull(pid, "The new progress should have a pid assigned.");
        progresses.add(progress);
        return progress;
    }

    protected RefreshProgress pivotData(RefreshProgress progress) {
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
        for (RefreshProgress progress: progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
        getBaseSourceTestBean().cleanupProgressTables();
    }

    protected void verifyResultTable() { }
}
