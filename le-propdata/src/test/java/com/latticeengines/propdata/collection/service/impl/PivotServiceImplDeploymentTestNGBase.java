package com.latticeengines.propdata.collection.service.impl;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.PivotProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.Source;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionDeploymentTestNGBase;

abstract public class PivotServiceImplDeploymentTestNGBase extends PropDataCollectionDeploymentTestNGBase {

    PivotService pivotService;
    PivotProgressEntityMgr progressEntityMgr;
    PivotedSource source;
    Source baseSource;
    Collection<PivotProgress> progresses = new HashSet<>();
    String baseSourceSplitColumn;

    abstract PivotService getPivotService();
    abstract PivotProgressEntityMgr getProgressEntityMgr();
    abstract PivotedSource getSource();
    abstract String getBaseSourceSplitColumn();
    abstract String testingBaseSourceTable();

    @BeforeMethod(groups = "deployment")
    public void setUp() throws Exception {
        hdfsPathBuilder.changeHdfsPodId("DeploymentTest");
        pivotService = getPivotService();
        progressEntityMgr = getProgressEntityMgr();
        source = getSource();
        baseSource = source.getBaseSource();
        baseSourceSplitColumn = getBaseSourceSplitColumn();
    }

    @AfterMethod(groups = "deployment")
    public void tearDown() throws Exception { }

    @Test(groups = "deployment")
    public void testWholeProgress() {
        truncateDestTable();
        downloadSnapshot();

        PivotProgress progress = createNewProgress(new Date());
        progress = pivotData(progress);
        exportToDB(progress);

        verifyResultTable(progress);

        cleanupProgressTables();
    }

    private void truncateDestTable() {
        String tableName = source.getTableName();
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) TRUNCATE TABLE " + tableName);
    }

    private void downloadSnapshot() {
        String targetDir = hdfsPathBuilder.constructRawDataFlowSnapshotDir(baseSource).toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetDir)) {
                HdfsUtils.rmdir(yarnConfiguration, targetDir);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        importFromCollectionDB(testingBaseSourceTable(), targetDir, "PropData-FunctionalTest",
                baseSourceSplitColumn, null);
    }

    protected PivotProgress createNewProgress(Date pivotDate) {
        PivotProgress progress = pivotService.startNewProgress(pivotDate, progressCreator);
        Assert.assertNotNull(progress, "Should have a progress in the job context.");
        Long pid = progress.getPid();
        Assert.assertNotNull(pid, "The new progress should have a pid assigned.");
        progresses.add(progress);
        return progress;
    }

    protected PivotProgress pivotData(PivotProgress progress) {
        PivotProgress response = pivotService.pivot(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.PIVOTED);

        PivotProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.PIVOTED);

        return response;
    }

    protected PivotProgress exportToDB(PivotProgress progress) {
        PivotProgress response = pivotService.exportToDB(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.UPLOADED);

        PivotProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.UPLOADED);

        return response;
    }

    protected void cleanupProgressTables() {
        for (PivotProgress progress: progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
    }

    protected void verifyResultTable(PivotProgress progress) {
        int rowsInPivotedTable = jdbcTemplateCollectionDB.queryForObject(
                "SELECT COUNT(*) FROM [" + source.getTableName() + "]", Integer.class);
        Assert.assertTrue(rowsInPivotedTable > 0,
                String.format("Only %d results in %s.", rowsInPivotedTable, source.getTableName()));
        Assert.assertEquals(rowsInPivotedTable, (int) progress.getRowsGenerated());
    }
}
