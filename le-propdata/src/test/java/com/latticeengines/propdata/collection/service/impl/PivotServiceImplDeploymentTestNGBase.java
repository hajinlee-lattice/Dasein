package com.latticeengines.propdata.collection.service.impl;

import java.io.InputStream;
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
    String baseSourceVersion = "current";

    abstract PivotService getPivotService();
    abstract PivotProgressEntityMgr getProgressEntityMgr();
    abstract PivotedSource getSource();

    @BeforeMethod(groups = "deployment")
    public void setUp() throws Exception {
        hdfsPathBuilder.changeHdfsPodId("DeploymentTestPivotService");
        pivotService = getPivotService();
        progressEntityMgr = getProgressEntityMgr();
        source = getSource();
        baseSource = source.getBaseSource();
    }

    @AfterMethod(groups = "deployment")
    public void tearDown() throws Exception { }

    @Test(groups = "deployment")
    public void testWholeProgress() {
        truncateDestTable();
        uploadBaseAvro();

        PivotProgress progress = createNewProgress(new Date());
        progress = pivotData(progress);
        progress = exportToDB(progress);
        finish(progress);

        verifyResultTable(progress);

        cleanupProgressTables();
    }

    private void truncateDestTable() {
        String tableName = source.getSqlTableName();
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) TRUNCATE TABLE " + tableName);
    }

    private void uploadBaseAvro() {
        InputStream baseAvroStream = ClassLoader.getSystemResourceAsStream("sources/" + baseSource.getSourceName() + ".avro");
        String targetPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).append("part-0000.avro").toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetPath);
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseAvroStream, targetPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected PivotProgress createNewProgress(Date pivotDate) {
        PivotProgress progress = pivotService.startNewProgress(pivotDate, baseSourceVersion, progressCreator);
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

    protected PivotProgress finish(PivotProgress progress) {
        PivotProgress response = pivotService.finish(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.FINISHED);

        PivotProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.FINISHED);

        return response;
    }

    protected void cleanupProgressTables() {
        for (PivotProgress progress: progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
    }

    protected void verifyResultTable(PivotProgress progress) {
        int rowsInPivotedTable = jdbcTemplateCollectionDB.queryForObject(
                "SELECT COUNT(*) FROM [" + source.getSqlTableName() + "]", Integer.class);
        Assert.assertTrue(rowsInPivotedTable > 0,
                String.format("Only %d results in %s.", rowsInPivotedTable, source.getSqlTableName()));
        Assert.assertEquals(rowsInPivotedTable, (int) progress.getRowsGenerated());
    }
}
