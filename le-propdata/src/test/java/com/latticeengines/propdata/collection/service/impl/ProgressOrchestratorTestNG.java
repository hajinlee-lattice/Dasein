package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.HdfsSourceEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.CollectedArchiveService;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.collection.source.CollectedSource;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.RawSource;
import com.latticeengines.propdata.collection.source.ServingSource;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;


@Component
public class ProgressOrchestratorTestNG extends PropDataCollectionFunctionalTestNGBase {

    @Autowired
    private ProgressOrchestrator orchestrator;

    @Autowired
    private ArchiveProgressEntityMgr archiveProgressEntityMgr;

    @Autowired
    private RefreshProgressEntityMgr refreshProgressEntityMgr;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    @Qualifier("testArchiveService")
    private CollectedArchiveService collectedArchiveService;

    @Autowired
    @Qualifier("testPivotService")
    private PivotService pivotService;

    @Autowired
    @Qualifier(value = "testCollectedSource")
    CollectedSource collectedSource;

    @Autowired
    @Qualifier(value = "testPivotedSource")
    PivotedSource pivotedSource;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        removeTestProgresses();
        scanOnlyTestingSources();
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        removeTestProgresses();
    }

    @Test(groups = "functional", enabled = false)
    public void testArchiveProgress() {
        CollectedSource testSource = collectedSource;

        ArchiveProgress progress = orchestrator.findArchiveProgressToProceed(testSource);
        Assert.assertNull(progress, "Should have no progress to proceed at beginning.");

        // create a new progress 1
        ArchiveProgress progress1 =
                archiveProgressEntityMgr.insertNewProgress(testSource, new Date(), new Date(), "PropDataFunctionalTest");
        progress = orchestrator.findArchiveProgressToProceed(testSource);
        Assert.assertNotNull(progress, "Should find the new progress.");
        Assert.assertEquals(progress.getRootOperationUID(), progress1.getRootOperationUID());

        // change 1 to a running state
        progress1.setStatus(ProgressStatus.TRANSFORMING);
        archiveProgressEntityMgr.updateProgress(progress1);
        progress = orchestrator.findArchiveProgressToProceed(testSource);
        Assert.assertNull(progress, "Should not find the running job.");

        // change 1 to intermediate step
        progress1.setStatus(ProgressStatus.TRANSFORMED);
        archiveProgressEntityMgr.updateProgress(progress1);
        progress = orchestrator.findArchiveProgressToProceed(testSource);
        Assert.assertNotNull(progress, "Should find the job.");
        Assert.assertEquals(progress.getRootOperationUID(), progress1.getRootOperationUID());

        // create a second new progress 2
        ArchiveProgress progress2 =
                archiveProgressEntityMgr.insertNewProgress(testSource, new Date(), new Date(), "PropDataFunctionalTest");
        progress = orchestrator.findArchiveProgressToProceed(testSource);
        Assert.assertNotNull(progress, "Should find the progress 1.");
        Assert.assertEquals(progress.getRootOperationUID(), progress1.getRootOperationUID());

        // change 1 to running
        progress1.setStatus(ProgressStatus.UPLOADING);
        archiveProgressEntityMgr.updateProgress(progress1);
        progress = orchestrator.findArchiveProgressToProceed(testSource);
        Assert.assertNull(progress, "Should not find the new job.");

        // change 1 to finished
        progress1.setStatus(ProgressStatus.FINISHED);
        archiveProgressEntityMgr.updateProgress(progress1);
        progress = orchestrator.findArchiveProgressToProceed(testSource);
        Assert.assertNotNull(progress, "Should find the second new progress.");
        Assert.assertEquals(progress.getRootOperationUID(), progress2.getRootOperationUID());

        // change 1 to failed with no retry
        progress1.setStatus(ProgressStatus.FAILED);
        progress1.setStatusBeforeFailed(ProgressStatus.UPLOADING);
        archiveProgressEntityMgr.updateProgress(progress1);
        progress = orchestrator.findArchiveProgressToProceed(testSource);
        Assert.assertNotNull(progress, "Should ignore the failed progress.");
        Assert.assertEquals(progress.getRootOperationUID(), progress2.getRootOperationUID());

        // change 2 to finished
        progress2.setStatus(ProgressStatus.FINISHED);
        archiveProgressEntityMgr.updateProgress(progress2);
        progress = orchestrator.findArchiveProgressToProceed(testSource);
        Assert.assertNotNull(progress, "Should find the failed progress.");
        Assert.assertEquals(progress.getRootOperationUID(), progress1.getRootOperationUID());

        // change 1 to failed with too many retries
        progress1.setNumRetries(3);
        archiveProgressEntityMgr.updateProgress(progress1);
        progress = orchestrator.findArchiveProgressToProceed(testSource);
        Assert.assertNull(progress, "Should ignore failed with too many retries.");

        archiveProgressEntityMgr.deleteProgressByRootOperationUid(progress1.getRootOperationUID());
        archiveProgressEntityMgr.deleteProgressByRootOperationUid(progress2.getRootOperationUID());
    }

    @Test(groups = "functional", enabled = false)
    public void testPivotProgress() {
        PivotedSource testSource = pivotedSource;
        CollectedSource testSource1 = collectedSource;

        // auto start 1
        String version1 = "version1";
        hdfsSourceEntityMgr.setCurrentVersion(testSource1, version1);
        RefreshProgress progress1 = orchestrator.findRefreshProgressToProceed(testSource);
        Assert.assertNotNull(progress1, "Should automatically start a new progress.");
        Assert.assertEquals(progress1.getBaseSourceVersion(), version1);

        // change 1 to running state
        progress1.setStatus(ProgressStatus.TRANSFORMING);
        refreshProgressEntityMgr.updateProgress(progress1);
        RefreshProgress progress = orchestrator.findRefreshProgressToProceed(testSource);
        Assert.assertNull(progress, "Should not find the running job.");

        // change 1 to intermediate state
        progress1.setStatus(ProgressStatus.TRANSFORMED);
        refreshProgressEntityMgr.updateProgress(progress1);
        progress = orchestrator.findRefreshProgressToProceed(testSource);
        Assert.assertNotNull(progress, "Should find the running job.");
        Assert.assertEquals(progress.getRootOperationUID(), progress1.getRootOperationUID());

        // change base version
        String version2 = "version2";
        hdfsSourceEntityMgr.setCurrentVersion(testSource1, version2);
        progress = orchestrator.findRefreshProgressToProceed(testSource);
        Assert.assertNotNull(progress, "Should find still the progress 1.");
        Assert.assertEquals(progress.getRootOperationUID(), progress1.getRootOperationUID());

        // change 1 to failed
        progress.setStatus(ProgressStatus.FAILED);
        refreshProgressEntityMgr.updateProgress(progress);
        Assert.assertNotNull(progress, "Should find the running job.");
        Assert.assertEquals(progress.getRootOperationUID(), progress1.getRootOperationUID());

        // change 1 to failed too many
        progress.setStatus(ProgressStatus.FAILED);
        progress.setNumRetries(3);
        refreshProgressEntityMgr.updateProgress(progress);
        RefreshProgress progress2 = orchestrator.findRefreshProgressToProceed(testSource);
        Assert.assertNotNull(progress2, "Should automatically pick up the new version.");
        Assert.assertEquals(progress2.getBaseSourceVersion(), version2, "Should pick up the new version.");

        // change 1 to finished
        progress1.setStatus(ProgressStatus.FINISHED);
        refreshProgressEntityMgr.updateProgress(progress1);
        progress = orchestrator.findRefreshProgressToProceed(testSource);
        Assert.assertNotNull(progress, "Should automatically pick up the new version.");
        Assert.assertEquals(progress.getRootOperationUID(), progress2.getRootOperationUID());

        refreshProgressEntityMgr.deleteProgressByRootOperationUid(progress1.getRootOperationUID());
        refreshProgressEntityMgr.deleteProgressByRootOperationUid(progress2.getRootOperationUID());
    }

    private void removeTestProgresses() {
        archiveProgressEntityMgr.deleteAllProgressesOfSource(collectedSource);
        refreshProgressEntityMgr.deleteAllProgressesOfSource(pivotedSource);
    }

    private void scanOnlyTestingSources () {
        Map<RawSource, ArchiveService> archiveServiceMap = new HashMap<>();
        Map<ServingSource, RefreshService> pivotServiceMap = new HashMap<>();
        archiveServiceMap.put(collectedSource, collectedArchiveService);
        pivotServiceMap.put(pivotedSource, pivotService);
        orchestrator.setServiceMaps(archiveServiceMap, pivotServiceMap);
    }

}
