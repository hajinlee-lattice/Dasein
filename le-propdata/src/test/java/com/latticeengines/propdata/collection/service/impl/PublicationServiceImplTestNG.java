package com.latticeengines.propdata.collection.service.impl;

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
import com.latticeengines.propdata.collection.entitymgr.PublicationEntityMgr;
import com.latticeengines.propdata.collection.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PublicationService;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.source.impl.BuiltWithPivoted;

public class PublicationServiceImplTestNG extends PropDataCollectionFunctionalTestNGBase {

    public static final String POD_ID = "PublicationServiceImplTestNG";
    public static final String PUBLICATION_NAME = "TestPublication";
    public static final String CURRENT_VERSION = "version1";
    public static final String SUBMITTER = PublicationServiceImplTestNG.class.getSimpleName();

    @Autowired
    private BuiltWithPivoted source;

    @Autowired
    private PublicationService publicationService;

    @Autowired
    private PublicationProgressEntityMgr progressEntityMgr;

    @Autowired
    private PublicationEntityMgr publicationEntityMgr;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    private Publication publication;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(POD_ID);
        hdfsSourceEntityMgr.setCurrentVersion(source, CURRENT_VERSION);
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
        List<PublicationProgress> progresses = publicationService.publishLatest(source, SUBMITTER);
        Assert.assertTrue(progresses.isEmpty(), "Should not allow new progress when there is already one");

        PublicationProgress progress1 = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                CURRENT_VERSION);
        publicationService.update(progress1).retry().retry().retry()
                .status(PublicationProgress.Status.FAILED).commit();
        progresses = publicationService.publishLatest(source, CURRENT_VERSION);
        Assert.assertFalse(progresses.isEmpty(),
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
        List<PublicationProgress> progressList = publicationService.scanNonTerminalProgresses();
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
        publication.setSourceName(source.getSourceName());
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
