package com.latticeengines.datacloud.etl.publication.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressService;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.Publication.MaterialType;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.SqlDestination;

public class PublicationProgressServiceImplTestNG extends DataCloudEtlFunctionalTestNGBase {

    private static final String POD_ID = "PublicationServiceImplTestNG";
    private static final String PUBLICATION_NAME = "TestPublication";
    private static final String CURRENT_VERSION = "version1";
    private static final String SUBMITTER = PublicationProgressServiceImplTestNG.class.getSimpleName();

    @Autowired
    private PublicationProgressService publicationProgressService;

    @Autowired
    private PublicationProgressEntityMgr progressEntityMgr;

    @Autowired
    private PublicationEntityMgr publicationEntityMgr;

    private Publication publication;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(POD_ID);
        publicationEntityMgr.removePublication(PUBLICATION_NAME);
        publication = getPublication();
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        publicationEntityMgr.removePublication(PUBLICATION_NAME);
    }

    @Test(groups = "functional")
    public void testCanFindExistingProgress() throws InterruptedException {
        progressEntityMgr.startNewProgress(publication, getDestination(), CURRENT_VERSION, SUBMITTER);
        Thread.sleep(2000);
        PublicationProgress progress2 = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                CURRENT_VERSION);
        Assert.assertNotNull(progress2, "Should find the existing progress");
    }

    @Test(groups = "functional", dependsOnMethods = "testCanFindExistingProgress")
    public void testCheckNewProgress() throws InterruptedException {
        PublicationProgress progress = publicationProgressService.publishVersion(publication, CURRENT_VERSION, SUBMITTER);
        Assert.assertNull(progress, "Should not allow new progress when there is already one");

        PublicationProgress progress1 = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                CURRENT_VERSION);
        publicationProgressService.update(progress1).retry().retry().retry()
                .status(ProgressStatus.FAILED).commit();
        Thread.sleep(2000);
        progress = publicationProgressService.publishVersion(publication, CURRENT_VERSION, CURRENT_VERSION);
        Assert.assertNotNull(progress,
                "Should allow new progress when the old one exceed max retry and is in FAILED status.");
    }

    @Test(groups = "functional", dependsOnMethods = "testCheckNewProgress")
    public void testForeignKeyCascading() {
        Publication publication1 = publicationEntityMgr.findByPublicationName(PUBLICATION_NAME);
        List<PublicationProgress> progresses = progressEntityMgr.findAllForPublication(publication1);
        Assert.assertFalse(progresses.isEmpty(), "Should have at least one progress.");
    }

    @Test(groups = "functional", dependsOnMethods = "testCheckNewProgress")
    public void testFindNonTerminalProgress() {
        List<PublicationProgress> progressList = publicationProgressService.scanNonTerminalProgresses();
        Assert.assertFalse(progressList.isEmpty(), "Should have at least one non-terminal progress");
        Boolean foundExpectedOne = false;
        for (PublicationProgress progress : progressList) {
            if (PUBLICATION_NAME.equals(progress.getPublication().getPublicationName())
                    && ProgressStatus.NEW.equals(progress.getStatus())) {
                foundExpectedOne = true;
            }
        }
        Assert.assertTrue(foundExpectedOne, "Should found the NEW progress just created.");
    }

    private Publication getPublication() {
        Publication publication = new Publication();
        publication.setPublicationName(PUBLICATION_NAME);
        publication.setSourceName("TestSource");
        publication.setNewJobMaxRetry(3);
        publication.setPublicationType(Publication.PublicationType.SQL);
        publication.setMaterialType(MaterialType.INGESTION);
        PublishToSqlConfiguration configuration = new PublishToSqlConfiguration();
        configuration.setDefaultTableName("DefaultTable");
        configuration.setPublicationStrategy(PublicationConfiguration.PublicationStrategy.VERSIONED);
        publication.setDestinationConfiguration(configuration);
        publication.setSchedularEnabled(false);

        return publicationEntityMgr.addPublication(publication);
    }

    private SqlDestination getDestination() {
        SqlDestination destination = new SqlDestination();
        destination.setTableName("Table1");
        return destination;
    }

}
