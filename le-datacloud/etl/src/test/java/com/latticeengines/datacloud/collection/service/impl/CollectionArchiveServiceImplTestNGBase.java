package com.latticeengines.datacloud.collection.service.impl;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.CollectedArchiveService;
import com.latticeengines.datacloud.collection.testframework.DataCloudCollectionFunctionalTestNGBase;
import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

// DataCloud SQL Servers are shutdown. Disable related tests.
@Deprecated
public abstract class CollectionArchiveServiceImplTestNGBase extends DataCloudCollectionFunctionalTestNGBase {

    CollectedArchiveService collectedArchiveService;
    ArchiveProgressEntityMgr progressEntityMgr;
    CollectedSource source;
    Calendar calendar = GregorianCalendar.getInstance();
    Date[] dates;
    Set<ArchiveProgress> progresses = new HashSet<>();

    abstract CollectedArchiveService getCollectedArchiveService();

    abstract ArchiveProgressEntityMgr getProgressEntityMgr();

    abstract CollectedSource getSource();

    abstract Date[] getDates();

    @BeforeMethod(groups = "collection", enabled = false)
    public void setUp() throws Exception {
        source = getSource();
        prepareCleanPod(source);
        setupBeans();
    }

    void setupBeans() {
        source = getSource();
        collectedArchiveService = getCollectedArchiveService();
        progressEntityMgr = getProgressEntityMgr();
        dates = getDates();
    }

    @AfterMethod(groups = "collection", enabled = false)
    public void tearDown() throws Exception {
    }

    @Test(groups = "collection", enabled = false)
    public void testWholeProgress() {
        purgeRawData();

        ArchiveProgress progress = createNewProgress(dates[0], dates[1]);
        progress = importFromDB(progress);
        finish(progress);

        testAutoDetermineDateRange();

        progress = createNewProgress(dates[1], dates[2]);
        progress = importFromDB(progress);
        finish(progress);

        cleanupProgressTables();
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

    protected void testAutoDetermineDateRange() {
    }

    ArchiveProgress createNewProgress(Date startDate, Date endDate) {
        ArchiveProgress progress = collectedArchiveService.startNewProgress(startDate, endDate, progressCreator);
        Assert.assertNotNull(progress, "Should have a progress in the job context.");
        Long pid = progress.getPid();
        Assert.assertNotNull(pid, "The new progress should have a pid assigned.");
        progresses.add(progress);
        return progress;
    }

    ArchiveProgress importFromDB(ArchiveProgress progress) {
        ArchiveProgress response = collectedArchiveService.importFromDB(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.DOWNLOADED);

        ArchiveProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.DOWNLOADED);

        return response;
    }

    ArchiveProgress finish(ArchiveProgress progress) {
        ArchiveProgress response = collectedArchiveService.finish(progress);

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
