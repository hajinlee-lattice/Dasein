package com.latticeengines.datacloud.collection.service.impl;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.collection.testframework.DataCloudCollectionFunctionalTestNGBase;
import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.MostRecentSource;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

public abstract class MostRecentServiceImplTestNGBase extends DataCloudCollectionFunctionalTestNGBase {

    private RefreshService refreshService;
    private RefreshProgressEntityMgr progressEntityMgr;
    private MostRecentSource source;
    private CollectedSource baseSource;
    private Date[] dates;
    private Collection<RefreshProgress> progresses = new HashSet<>();

    abstract RefreshService getRefreshService();

    abstract RefreshProgressEntityMgr getProgressEntityMgr();

    abstract MostRecentSource getSource();

    abstract CollectionArchiveServiceImplTestNGBase getBaseSourceTestBean();

    abstract Integer getExpectedRows();

    @BeforeMethod(groups = "collection", enabled = false)
    public void setUp() throws Exception {
        source = getSource();
        prepareCleanPod(source);
        getBaseSourceTestBean().setupBeans();

        refreshService = getRefreshService();
        progressEntityMgr = getProgressEntityMgr();
        baseSource = (CollectedSource) source.getBaseSources()[0];
        dates = getBaseSourceTestBean().getDates();
    }

    @AfterMethod(groups = "collection", enabled = false)
    public void tearDown() throws Exception {
        getBaseSourceTestBean().tearDown();
    }

    @Test(groups = "collection")
    public void testWholeProgress() {
        ArchiveProgress archiveProgress;
        RefreshProgress progress;

        getBaseSourceTestBean().purgeRawData();
        archiveProgress = getBaseSourceTestBean().createNewProgress(dates[0], dates[1]);
        getBaseSourceTestBean().importFromDB(archiveProgress);
        getBaseSourceTestBean().finish(archiveProgress);

        progress = createNewProgress(dates[1]);
        progress = transformData(progress);
        progress = exportToDB(progress);
        finish(progress);

        archiveProgress = getBaseSourceTestBean().createNewProgress(dates[1], dates[2]);
        getBaseSourceTestBean().importFromDB(archiveProgress);
        getBaseSourceTestBean().finish(archiveProgress);

        progress = createNewProgress(dates[2]);
        progress = transformData(progress);
        progress = exportToDB(progress);
        finish(progress);

        verifyResultTable(progress);
        cleanupProgressTables();
    }

    protected RefreshProgress createNewProgress(Date pivotDate) {
        RefreshProgress progress = refreshService.startNewProgress(pivotDate, null, progressCreator);
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

    protected void verifyResultTable(RefreshProgress progress) {
        if (getSource() instanceof HasSqlPresence) {
            verifyUniqueness();
            verifyMostRecent();
        }

        verifyNumberOfRows(progress);
    }

    protected void verifyUniqueness() {
        int maxMultiplicity = jdbcTemplateCollectionDB.queryForObject(
                "SELECT TOP 1 COUNT(*) FROM " + ((HasSqlPresence) source).getSqlTableName() + " GROUP BY "
                        + StringUtils.join(getSource().getPrimaryKey(), ",") + " ORDER BY COUNT(*) DESC",
                Integer.class);
        Assert.assertEquals(maxMultiplicity, 1, "Each unique key should have one record.");
    }

    protected void verifyMostRecent() {
        String sql = "SELECT COUNT(*) FROM " + ((HasSqlPresence) source).getSqlTableName() + " lhs \n" + "INNER JOIN "
                + baseSource.getCollectedTableName() + " rhs\n ON ";
        for (String key : source.getPrimaryKey()) {
            sql += "lhs.[" + key + "] = rhs.[" + key + "]\n AND ";
        }
        sql += "lhs.[" + source.getTimestampField() + "] < rhs.[" + source.getTimestampField() + "]";
        int outdatedRows = jdbcTemplateCollectionDB.queryForObject(sql, Integer.class);
        Assert.assertEquals(outdatedRows, 0, "There are " + outdatedRows + " rows outdated.");
    }

    protected void verifyNumberOfRows(RefreshProgress progress) {
        Assert.assertEquals(progress.getRowsGeneratedInHdfs(), (int) getExpectedRows());
        if (getSource() instanceof HasSqlPresence) {
            int rowsInPivotedTable = jdbcTemplateCollectionDB.queryForObject(
                    "SELECT COUNT(*) FROM [" + ((HasSqlPresence) source).getSqlTableName() + "]", Integer.class);
            Assert.assertTrue(rowsInPivotedTable > 0, String.format("Only %d results in %s.", rowsInPivotedTable,
                    ((HasSqlPresence) source).getSqlTableName()));
            Assert.assertEquals(rowsInPivotedTable, (int) progress.getRowsGeneratedInHdfs());
        }
    }

}
