package com.latticeengines.propdata.collection.service.impl;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.MostRecentSource;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;

abstract public class MostRecentServiceImplTestNGBase extends PropDataCollectionFunctionalTestNGBase {

    private static final String testPod = "FunctionalMostRecent";

    RefreshService refreshService;
    RefreshProgressEntityMgr progressEntityMgr;
    MostRecentSource source;
    CollectedSource baseSource;
    Date[] dates;
    Collection<RefreshProgress> progresses = new HashSet<>();

    abstract RefreshService getRefreshService();
    abstract RefreshProgressEntityMgr getProgressEntityMgr();
    abstract MostRecentSource getSource();
    abstract CollectionArchiveServiceImplTestNGBase getBaseSourceTestBean();

    @BeforeMethod(groups = "functional.source")
    public void setUp() throws Exception {
        source = getSource();
        hdfsPathBuilder.changeHdfsPodId(testPod + source.getSourceName());
        getBaseSourceTestBean().setUpPod(testPod + source.getSourceName());

        refreshService = getRefreshService();
        progressEntityMgr = getProgressEntityMgr();
        baseSource = source.getBaseSources()[0];
        dates = getBaseSourceTestBean().getDates();
    }

    @AfterMethod(groups = "functional.source")
    public void tearDown() throws Exception {
        getBaseSourceTestBean().tearDown();
    }

    @Test(groups = "functional.source")
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

        verifyResultTable();
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
        for (RefreshProgress progress: progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
        getBaseSourceTestBean().cleanupProgressTables();
    }

    protected void verifyResultTable() {
        if (source instanceof HasSqlPresence) {
            verifyUniqueness();
            verifyMostRecent();
        }
    }

    protected void verifyUniqueness() {
        int maxMultiplicity = jdbcTemplateCollectionDB.queryForObject("SELECT TOP 1 COUNT(*) FROM "
                + ((HasSqlPresence) source).getSqlTableName() + " GROUP BY " + StringUtils.join(getSource().getPrimaryKey(), ",")
                + " ORDER BY COUNT(*) DESC", Integer.class);
        Assert.assertEquals(maxMultiplicity, 1, "Each unique key should have one record.");
    }

    protected void verifyMostRecent() {
        String sql = "SELECT COUNT(*) FROM " + ((HasSqlPresence) source).getSqlTableName() + " lhs \n"
                + "INNER JOIN " + baseSource.getCollectedTableName() + " rhs\n ON ";
        for (String key: source.getPrimaryKey()) {
            sql += "lhs.[" + key + "] = rhs.[" + key + "]\n AND ";
        }
        sql += "lhs.[" + source.getTimestampField() + "] < rhs.[" + source.getTimestampField() + "]";
        int outdatedRows = jdbcTemplateCollectionDB.queryForObject(sql, Integer.class);
        Assert.assertEquals(outdatedRows, 0, "There are " + outdatedRows + " rows outdated.");
    }
}
