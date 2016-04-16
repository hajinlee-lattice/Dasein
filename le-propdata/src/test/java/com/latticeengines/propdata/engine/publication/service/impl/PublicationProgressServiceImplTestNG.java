package com.latticeengines.propdata.engine.publication.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.propdata.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.propdata.publication.SqlDestination;
import com.latticeengines.propdata.engine.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.propdata.engine.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.propdata.engine.publication.service.PublicationProgressService;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;

public class PublicationProgressServiceImplTestNG extends PropDataCollectionFunctionalTestNGBase {

    public static final String POD_ID = "PublicationServiceImplTestNG";
    public static final String PUBLICATION_NAME = "TestPublication";
    public static final String CURRENT_VERSION = "version1";
    public static final String SUBMITTER = PublicationProgressServiceImplTestNG.class.getSimpleName();

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
    public void testCanFindExistingProgress() {
        progressEntityMgr.startNewProgress(publication, getDestination(), CURRENT_VERSION, SUBMITTER);
        PublicationProgress progress2 = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                CURRENT_VERSION);
        Assert.assertNotNull(progress2, "Should find the existing progress");
    }

    @Test(groups = "functional", dependsOnMethods = "testCanFindExistingProgress")
    public void testCheckNewProgress() {
        PublicationProgress progress = publicationProgressService.publishVersion(publication, CURRENT_VERSION, SUBMITTER);
        Assert.assertNull(progress, "Should not allow new progress when there is already one");

        PublicationProgress progress1 = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                CURRENT_VERSION);
        publicationProgressService.update(progress1).retry().retry().retry()
                .status(PublicationProgress.Status.FAILED).commit();
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
                    && PublicationProgress.Status.NEW.equals(progress.getStatus())) {
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

        PublishToSqlConfiguration configuration = new PublishToSqlConfiguration();
        configuration.setDefaultTableName("DefaultTable");
        configuration.setPublicationStrategy(PublishToSqlConfiguration.PublicationStrategy.VERSIONED);
        publication.setDestinationConfiguration(configuration);

        return publicationEntityMgr.addPublication(publication);
    }

    private SqlDestination getDestination() {
        SqlDestination destination = new SqlDestination();
        destination.setTableName("Table1");
        return destination;
    }

}
