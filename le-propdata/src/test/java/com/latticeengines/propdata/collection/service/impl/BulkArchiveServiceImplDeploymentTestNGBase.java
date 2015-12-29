package com.latticeengines.propdata.collection.service.impl;

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.BulkArchiveService;
import com.latticeengines.propdata.collection.source.BulkSource;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionDeploymentTestNGBase;

abstract public class BulkArchiveServiceImplDeploymentTestNGBase extends PropDataCollectionDeploymentTestNGBase {

    BulkArchiveService archiveService;
    ArchiveProgressEntityMgr progressEntityMgr;
    BulkSource source;
    Set<ArchiveProgress> progresses = new HashSet<>();

    abstract BulkArchiveService getArchiveService();
    abstract ArchiveProgressEntityMgr getProgressEntityMgr();
    abstract BulkSource getSource();

    @BeforeMethod(groups = "deployment")
    public void setUp() throws Exception {
        setUpPod("DeploymentTestBulkArchiveService");
    }

    void setUpPod(String podId) {
        hdfsPathBuilder.changeHdfsPodId(podId);
        archiveService = getArchiveService();
        progressEntityMgr = getProgressEntityMgr();
        source = getSource();
    }

    @AfterMethod(groups = "deployment")
    public void tearDown() throws Exception { }

    @Test(groups = "deployment")
    public void testWholeProgress() {
        run();
        cleanupProgressTables();
    }

    public void run() {
        purgeRawData();
        ArchiveProgress progress = createNewProgress();
        progress = importFromDB(progress);
        finish(progress);
    }

    void purgeRawData() {
        try {
            String rawDir = hdfsPathBuilder.constructRawDir(source).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, rawDir)) {
                HdfsUtils.rmdir(yarnConfiguration, rawDir);
            }
        } catch (Exception e) {
            Assert.fail("Failed to purge raw data.", e);
        }
    }

    ArchiveProgress createNewProgress() {
        ArchiveProgress progress = archiveService.startNewProgress(progressCreator);
        Assert.assertNotNull(progress, "Should have a progress in the job context.");
        Long pid = progress.getPid();
        Assert.assertNotNull(pid, "The new progress should have a pid assigned.");
        progresses.add(progress);
        return progress;
    }

    ArchiveProgress importFromDB(ArchiveProgress progress) {
        ArchiveProgress response = archiveService.importFromDB(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.DOWNLOADED);

        ArchiveProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.DOWNLOADED);

        return response;
    }

    ArchiveProgress finish(ArchiveProgress progress) {
        ArchiveProgress response = archiveService.finish(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.FINISHED);

        ArchiveProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.FINISHED);

        return response;
    }

    void cleanupProgressTables() {
        for (ArchiveProgress progress: progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
    }
}
