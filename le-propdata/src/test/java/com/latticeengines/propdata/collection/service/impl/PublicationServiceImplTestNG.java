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
import com.latticeengines.propdata.collection.entitymgr.PublicationEntityMgr;
import com.latticeengines.propdata.collection.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PublicationProgressService;
import com.latticeengines.propdata.collection.service.PublicationService;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.source.impl.BuiltWithPivoted;

public class PublicationServiceImplTestNG extends PropDataCollectionFunctionalTestNGBase {

    public final String POD_ID = this.getClass().getSimpleName();
    public static final String PUBLICATION_NAME = "TestPublication2";
    public static final String CURRENT_VERSION = "version2";
    public final String SUBMITTER = this.getClass().getSimpleName();

    @Autowired
    private PublicationProgressService publicationProgressService;

    @Autowired
    private PublicationService publicationService;

    @Autowired
    private PublicationProgressEntityMgr progressEntityMgr;

    @Autowired
    private PublicationEntityMgr publicationEntityMgr;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private BuiltWithPivoted source;

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
    public void testPublish() {
        List<PublicationProgress> progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 0, "Should have zero progress at beginning");

        publicationService.publish(source, SUBMITTER);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 1, "Should have a new progress");

        publicationService.publish(source, SUBMITTER);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 1, "Should still have one progress");

        PublicationProgress progress1 = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                CURRENT_VERSION);
        publicationProgressService.update(progress1).retry().retry().retry()
                .status(PublicationProgress.Status.FAILED).commit();
        publicationService.publish(source, SUBMITTER);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 2, "Should have one more progress.");
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

}
