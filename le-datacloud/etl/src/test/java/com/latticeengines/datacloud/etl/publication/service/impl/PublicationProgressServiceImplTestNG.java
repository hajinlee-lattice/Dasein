package com.latticeengines.datacloud.etl.publication.service.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressService;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.Publication.MaterialType;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToDynamoConfiguration;

public class PublicationProgressServiceImplTestNG extends DataCloudEtlFunctionalTestNGBase {

    private static final String POD_ID = "PublicationServiceImplTestNG";
    private static final String DYNAMO_PUBLICATION_NAME = "TestDynamoLPublication";
    private static final String CURRENT_VERSION = "version1";
    private static final String SUBMITTER = PublicationProgressServiceImplTestNG.class.getSimpleName();

    @Inject
    private PublicationProgressService publicationProgressService;

    @Inject
    private PublicationEntityMgr publicationEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(POD_ID);
        publicationEntityMgr.removePublication(DYNAMO_PUBLICATION_NAME);
        createDynamoPublication();
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        publicationEntityMgr.removePublication(DYNAMO_PUBLICATION_NAME);
    }

    @Test(groups = "functional")
    public void testPublishVersionForDefaultDestination() {
        Publication publication = publicationEntityMgr.findByPublicationName(DYNAMO_PUBLICATION_NAME);
        PublicationProgress progress = publicationProgressService.publishVersion(publication, null, CURRENT_VERSION,
                SUBMITTER);
        Assert.assertNotNull(progress.getDestination());
    }

    private Publication createDynamoPublication() {
        Publication publication = new Publication();
        publication.setPublicationName(DYNAMO_PUBLICATION_NAME);
        publication.setSourceName(DataCloudConstants.ACCOUNT_MASTER);
        publication.setNewJobMaxRetry(3);
        publication.setPublicationType(Publication.PublicationType.DYNAMO);
        publication.setMaterialType(MaterialType.SOURCE);
        PublishToDynamoConfiguration configuration = new PublishToDynamoConfiguration();
        publication.setDestinationConfiguration(configuration);
        publication.setSchedularEnabled(false);

        return publicationEntityMgr.addPublication(publication);
    }

}
