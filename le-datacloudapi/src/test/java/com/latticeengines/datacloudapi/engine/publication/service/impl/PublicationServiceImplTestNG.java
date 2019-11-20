package com.latticeengines.datacloudapi.engine.publication.service.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ACCOUNT_MASTER_DIFF;
import static org.mockito.ArgumentMatchers.any;

import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressService;
import com.latticeengines.datacloudapi.engine.testframework.PropDataEngineFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.FakeApplicationId;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.Publication.MaterialType;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.DynamoDestination;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToDynamoConfiguration;

public class PublicationServiceImplTestNG extends PropDataEngineFunctionalTestNGBase {

    private static final String POD_ID = PublicationServiceImplTestNG.class.getSimpleName();
    private static final String PUBLICATION_NAME = "TestPublication2";
    private static final String CURRENT_VERSION = "version2";
    private final String SUBMITTER = this.getClass().getSimpleName();
    private static final ApplicationId FAKE_APPID = new FakeApplicationId("__FAKE_APPID__");

    @Inject
    private PublicationProgressService publicationProgressService;

    @Inject
    @Spy
    private PublicationServiceImpl publicationService;

    @Inject
    private PublicationProgressEntityMgr progressEntityMgr;

    @Inject
    private PublicationEntityMgr publicationEntityMgr;

    @Inject
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    private DataCloudVersionService dataCloudVersionService;

    private Publication publication;
    private PublicationRequest publicationRequest;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(POD_ID);
        hdfsSourceEntityMgr.setCurrentVersion(ACCOUNT_MASTER_DIFF, CURRENT_VERSION);
        publicationRequest = new PublicationRequest();
        publicationRequest.setSubmitter(SUBMITTER);
        publicationRequest.setSourceVersion(CURRENT_VERSION);

        MockitoAnnotations.initMocks(this);
        Mockito.doReturn(FAKE_APPID).when(publicationService).submitWorkflow(any());
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        // publicationEntityMgr.removePublication(PUBLICATION_NAME);
    }

    @BeforeMethod(groups = "functional")
    public void setupMethod() {
        publicationEntityMgr.removePublication(PUBLICATION_NAME);
        publication = getPublication();
    }

    @Test(groups = "functional")
    public void testPublish() throws Exception {
        List<PublicationProgress> progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 0, "Should have zero progress at beginning");

        publicationService.kickoff(PUBLICATION_NAME, publicationRequest);
        Thread.sleep(2000);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 1, "Should have a new progress");
        publicationService.kickoff(PUBLICATION_NAME, publicationRequest);
        Thread.sleep(2000);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 1, "Should still have one progress");

        PublicationProgress progress1 = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                CURRENT_VERSION);
        Assert.assertEquals(((DynamoDestination) (progress1.getDestination())).getVersion(),
                dataCloudVersionService.currentDynamoVersion(ACCOUNT_MASTER_DIFF));

        publicationProgressService.update(progress1).retry().retry().retry()
                .status(ProgressStatus.FAILED).commit();
        Thread.sleep(2000);
        publicationService.kickoff(PUBLICATION_NAME, publicationRequest);
        Thread.sleep(2000);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 2, "Should have one more progress.");
        // test status of latest publication progress with required version
        ProgressStatus status = publicationService.findProgressAtVersion(PUBLICATION_NAME, CURRENT_VERSION);
        Assert.assertEquals(status, ProgressStatus.NEW);

        // test publish()
        publicationProgressService.update(progress1).retry().retry().retry().status(ProgressStatus.FAILED).commit();
        Thread.sleep(2000);
        publicationService.publish(PUBLICATION_NAME, publicationRequest);
        progress1 = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication, CURRENT_VERSION);
        Assert.assertEquals(((DynamoDestination) (progress1.getDestination())).getVersion(),
                dataCloudVersionService.currentDynamoVersion(ACCOUNT_MASTER_DIFF));
    }

    private Publication getPublication() {
        Publication publication = new Publication();
        publication.setPublicationName(PUBLICATION_NAME);
        publication.setSourceName(ACCOUNT_MASTER_DIFF);
        publication.setNewJobMaxRetry(3);
        publication.setSchedularEnabled(false);
        publication.setMaterialType(MaterialType.SOURCE);
        publication.setPublicationType(Publication.PublicationType.DYNAMO);

        PublishToDynamoConfiguration config = new PublishToDynamoConfiguration();
        publication.setDestinationConfiguration(config);

        return publicationEntityMgr.addPublication(publication);
    }

}
