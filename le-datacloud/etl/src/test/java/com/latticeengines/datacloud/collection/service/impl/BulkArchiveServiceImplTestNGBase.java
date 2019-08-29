package com.latticeengines.datacloud.collection.service.impl;

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.BulkArchiveService;
import com.latticeengines.datacloud.collection.testframework.DataCloudCollectionFunctionalTestNGBase;
import com.latticeengines.datacloud.core.source.BulkSource;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

//DataCloud SQL Servers are shutdown. Disable related tests.
@Deprecated
public abstract class BulkArchiveServiceImplTestNGBase extends DataCloudCollectionFunctionalTestNGBase {

    BulkArchiveService archiveService;
    ArchiveProgressEntityMgr progressEntityMgr;
    BulkSource source;
    Set<ArchiveProgress> progresses = new HashSet<>();

    abstract BulkArchiveService getArchiveService();

    abstract ArchiveProgressEntityMgr getProgressEntityMgr();

    abstract BulkSource getSource();

    @BeforeMethod(groups = "collection", enabled = false)
    public void setUp() throws Exception {
        source = getSource();
        prepareCleanPod(source);
        setupBeans();
    }

    void setupBeans() {
        source = getSource();
        archiveService = getArchiveService();
        progressEntityMgr = getProgressEntityMgr();
    }

    @AfterMethod(groups = "collection", enabled = false)
    public void tearDown() throws Exception {
    }

    @Test(groups = "collection", enabled = false)
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
        for (ArchiveProgress progress : progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
    }
}
