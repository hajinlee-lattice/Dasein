package com.latticeengines.propdata.collection.service.impl;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.source.CollectionSource;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionDeploymentTestNGBase;

abstract public class ArchiveServiceImplDeploymentTestNGBase extends PropDataCollectionDeploymentTestNGBase {

    ArchiveService archiveService;
    ArchiveProgressEntityMgr progressEntityMgr;
    CollectionSource source;
    Calendar calendar = GregorianCalendar.getInstance();
    Date[] dates;
    Collection<ArchiveProgress> progresses = new HashSet<>();

    abstract ArchiveService getArchiveService();
    abstract ArchiveProgressEntityMgr getProgressEntityMgr();
    abstract CollectionSource getSource();
    abstract String[] uniqueColumns();
    // the test will first archive data between date[0] and date[1], the refresh by data between date[1] and date[2]
    abstract Date[] getDates();

    @BeforeMethod(groups = "deployment")
    public void setUp() throws Exception {
        hdfsPathBuilder.changeHdfsPodId("DeploymentTestArchiveService");
        archiveService = getArchiveService();
        progressEntityMgr = getProgressEntityMgr();
        dates = getDates();
        source = getSource();
    }

    @AfterMethod(groups = "deployment")
    public void tearDown() throws Exception { }

    @Test(groups = "deployment")
    public void testWholeProgress() {
        truncateDestTable();
        purgeRawData();

        ArchiveProgress progress = createNewProgress(dates[0], dates[1]);
        progress = importFromDB(progress);
        progress = transformRawData(progress);
        progress = exportToDB(progress);
        finish(progress);

        testAutoDetermineDateRange();

        progress = createNewProgress(dates[1], dates[2]);
        progress = importFromDB(progress);
        progress = transformRawData(progress);
        progress = exportToDB(progress);
        finish(progress);

        cleanupProgressTables();
    }

    private void purgeRawData() {
        try {
            String rawDir = hdfsPathBuilder.constructRawDir(source).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, rawDir)) {
                HdfsUtils.rmdir(yarnConfiguration, rawDir);
            }
        } catch (Exception e) {
            Assert.fail("Failed to purge raw data.", e);
        }
    }

    private void truncateDestTable() {
        String tableName = source.getTableName();
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) TRUNCATE TABLE " + tableName);
    }

    protected void testAutoDetermineDateRange() { }

    protected ArchiveProgress createNewProgress(Date startDate, Date endDate) {
        ArchiveProgress progress = archiveService.startNewProgress(startDate, endDate, progressCreator);
        Assert.assertNotNull(progress, "Should have a progress in the job context.");
        Long pid = progress.getPid();
        Assert.assertNotNull(pid, "The new progress should have a pid assigned.");
        progresses.add(progress);
        return progress;
    }

    protected ArchiveProgress importFromDB(ArchiveProgress progress) {
        ArchiveProgress response = archiveService.importFromDB(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.DOWNLOADED);

        ArchiveProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.DOWNLOADED);

        return response;
    }

    protected ArchiveProgress transformRawData(ArchiveProgress progress) {
        ArchiveProgress response = archiveService.transformRawData(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.TRANSFORMED);

        ArchiveProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.TRANSFORMED);

        return response;
    }

    protected ArchiveProgress exportToDB(ArchiveProgress progress) {
        ArchiveProgress response = archiveService.exportToDB(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.UPLOADED);

        ArchiveProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.UPLOADED);

        verifyUniqueness();

        return response;
    }

    protected ArchiveProgress finish(ArchiveProgress progress) {
        ArchiveProgress response = archiveService.finish(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.FINISHED);

        ArchiveProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.FINISHED);

        return response;
    }

    protected void cleanupProgressTables() {
        for (ArchiveProgress progress: progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
    }

    protected void verifyUniqueness() {
        int maxMultiplicity = jdbcTemplateCollectionDB.queryForObject("SELECT TOP 1 COUNT(*) FROM "
                + source.getTableName() + " GROUP BY " + StringUtils.join(uniqueColumns(), ",")
                + " ORDER BY COUNT(*) DESC", Integer.class);
        Assert.assertEquals(maxMultiplicity, 1, "Each unique key should have one record.");
    }
}
