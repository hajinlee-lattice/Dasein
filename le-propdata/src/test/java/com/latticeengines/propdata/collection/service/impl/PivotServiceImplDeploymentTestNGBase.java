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
import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.Source;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionDeploymentTestNGBase;

abstract public class PivotServiceImplDeploymentTestNGBase extends PropDataCollectionDeploymentTestNGBase {

    PivotService pivotService;
    RefreshProgressEntityMgr progressEntityMgr;
    PivotedSource source;
    Source baseSource;
    Collection<RefreshProgress> progresses = new HashSet<>();
    String baseSourceVersion = "current";

    abstract PivotService getPivotService();
    abstract RefreshProgressEntityMgr getProgressEntityMgr();
    abstract PivotedSource getSource();

    @BeforeMethod(groups = "deployment")
    public void setUp() throws Exception {
        hdfsPathBuilder.changeHdfsPodId("DeploymentTestPivot");
        pivotService = getPivotService();
        progressEntityMgr = getProgressEntityMgr();
        source = getSource();
        baseSource = source.getBaseSource();
    }

    @AfterMethod(groups = "deployment")
    public void tearDown() throws Exception { }

    @Test(groups = "deployment")
    public void testWholeProgress() {
        uploadBaseAvro();

        RefreshProgress progress = createNewProgress(new Date());
        progress = pivotData(progress);
        progress = exportToDB(progress);
        finish(progress);

        verifyResultTable(progress);

        cleanupProgressTables();
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

    protected RefreshProgress createNewProgress(Date pivotDate) {
        RefreshProgress progress = pivotService.startNewProgress(pivotDate, baseSourceVersion, progressCreator);
        Assert.assertNotNull(progress, "Should have a progress in the job context.");
        Long pid = progress.getPid();
        Assert.assertNotNull(pid, "The new progress should have a pid assigned.");
        progresses.add(progress);
        return progress;
    }

    protected RefreshProgress pivotData(RefreshProgress progress) {
        RefreshProgress response = pivotService.transform(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.TRANSFORMED);

        RefreshProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.TRANSFORMED);

        return response;
    }

    protected RefreshProgress exportToDB(RefreshProgress progress) {
        RefreshProgress response = pivotService.exportToDB(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.UPLOADED);

        RefreshProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.UPLOADED);

        return response;
    }

    protected RefreshProgress finish(RefreshProgress progress) {
        RefreshProgress response = pivotService.finish(progress);

        Assert.assertEquals(response.getStatus(), ProgressStatus.FINISHED);

        RefreshProgress progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.FINISHED);

        return response;
    }

    protected void cleanupProgressTables() {
        for (RefreshProgress progress: progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
    }

    protected void verifyResultTable(RefreshProgress progress) {
        int rowsInPivotedTable = jdbcTemplateCollectionDB.queryForObject(
                "SELECT COUNT(*) FROM [" + source.getSqlTableName() + "]", Integer.class);
        Assert.assertTrue(rowsInPivotedTable > 0,
                String.format("Only %d results in %s.", rowsInPivotedTable, source.getSqlTableName()));
        Assert.assertEquals(rowsInPivotedTable, (int) progress.getRowsGenerated());
    }
}
