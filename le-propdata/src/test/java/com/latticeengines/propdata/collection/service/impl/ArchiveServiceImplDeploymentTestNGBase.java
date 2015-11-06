package com.latticeengines.propdata.collection.service.impl;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressBase;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.CollectionJobContext;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;

abstract public class ArchiveServiceImplDeploymentTestNGBase<Progress extends ArchiveProgressBase>
        extends PropDataCollectionFunctionalTestNGBase {

    private static final String progressCreator = "DeploymentTest";

    ArchiveService archiveService;
    ArchiveProgressEntityMgr<Progress> progressEntityMgr;
    String sourceName;
    Calendar calendar = GregorianCalendar.getInstance();
    Date[] dates;
    Collection<Progress> progresses = new HashSet<>();

    abstract ArchiveService getArchiveService();
    abstract ArchiveProgressEntityMgr<Progress> getProgressEntityMgr();
    abstract String sourceName();
    abstract String[] uniqueColumns();

    // the test will first archive data between date[0] and date[1], the refresh by data between date[1] and date[2]
    abstract Date[] getDates();

    // tow dates, between which there is no raw data in the test set
    abstract Date[] getEmptyDataDates();

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplateDest")
    protected JdbcTemplate jdbcTemplateDest;

    @BeforeMethod(groups = "deployment")
    public void setUp() throws Exception {
        hdfsPathBuilder.changeHdfsPodId("DeploymentTest");
        archiveService = getArchiveService();
        progressEntityMgr = getProgressEntityMgr();
        dates = getDates();
        sourceName = sourceName();
    }

    @AfterMethod(groups = "deployment")
    public void tearDown() throws Exception { }

    @Test(groups = "deployment")
    public void testWholeProgress() {
        truncateDestTable();

        CollectionJobContext context = createNewProgress(dates[0], dates[1]);
        context = importFromDB(context);
        context = transformRawData(context);
        exportToDB(context);

        context = createNewProgress(dates[1], dates[2]);
        context = importFromDB(context);
        context = transformRawData(context);
        exportToDB(context);

        cleanupProgressTables();
    }

    @Test(groups = "deployment", dependsOnMethods = "testWholeProgress")
    public void testEmptyInput() {
        Date[] dates = getEmptyDataDates();

        CollectionJobContext context = createNewProgress(dates[0], dates[1]);
        context = importFromDB(context);
        context = transformRawData(context);
        exportToDB(context);

        cleanupProgressTables();
    }

    abstract String destTableName();

    private void truncateDestTable() {
        String tableName = destTableName();
        jdbcTemplateDest.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) TRUNCATE TABLE " + tableName);
    }

    private CollectionJobContext createNewProgress(Date startDate, Date endDate) {
        CollectionJobContext context = archiveService.startNewProgress(startDate, endDate, progressCreator);
        Progress progress = context.getProperty(CollectionJobContext.PROGRESS_KEY, progressEntityMgr.getProgressClass());
        Assert.assertNotNull(progress, "Should have a progress in the job context.");
        Long pid = progress.getPid();
        Assert.assertNotNull(pid, "The new progress should have a pid assigned.");

        progresses.add(progress);
        return context;
    }

    private CollectionJobContext importFromDB(CollectionJobContext request) {
        CollectionJobContext response = archiveService.importFromDB(request);

        Progress progressInCtx =
                request.getProperty(CollectionJobContext.PROGRESS_KEY, progressEntityMgr.getProgressClass());
        Assert.assertEquals(progressInCtx.getStatus(), ArchiveProgressStatus.DOWNLOADED);

        Progress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progressInCtx.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ArchiveProgressStatus.DOWNLOADED);

        return response;
    }

    private CollectionJobContext transformRawData(CollectionJobContext request) {
        CollectionJobContext response = archiveService.transformRawData(request);

        Progress progressInCtx =
                request.getProperty(CollectionJobContext.PROGRESS_KEY, progressEntityMgr.getProgressClass());
        Assert.assertEquals(progressInCtx.getStatus(), ArchiveProgressStatus.TRANSFORMED);

        Progress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progressInCtx.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ArchiveProgressStatus.TRANSFORMED);

        return response;
    }

    private CollectionJobContext exportToDB(CollectionJobContext request) {
        CollectionJobContext response = archiveService.exportToDB(request);

        Progress progressInCtx =
                request.getProperty(CollectionJobContext.PROGRESS_KEY, progressEntityMgr.getProgressClass());
        Assert.assertEquals(progressInCtx.getStatus(), ArchiveProgressStatus.UPLOADED);

        Progress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progressInCtx.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ArchiveProgressStatus.UPLOADED);

        verifyUniqueness();

        return response;
    }

    private void cleanupProgressTables() {
        for (Progress progress: progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
    }

    private void verifyUniqueness() {
        int maxMultiplicity = jdbcTemplateDest.queryForObject("SELECT TOP 1 COUNT(*) FROM " + destTableName() + " GROUP BY " +
                StringUtils.join(uniqueColumns(), ",")+ " ORDER BY COUNT(*) DESC", Integer.class);
        Assert.assertEquals(maxMultiplicity, 1, "Each unique key should have one record.");
    }
}
