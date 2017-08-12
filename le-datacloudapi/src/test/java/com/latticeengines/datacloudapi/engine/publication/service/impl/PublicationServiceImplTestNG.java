package com.latticeengines.datacloudapi.engine.publication.service.impl;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.impl.BuiltWithPivoted;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressService;
import com.latticeengines.datacloudapi.engine.publication.service.PublicationService;
import com.latticeengines.datacloudapi.engine.testframework.PropDataEngineFunctionalTestNGBase;
import com.latticeengines.datacloudapi.engine.testframework.PublicationWorkflowServlet;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.Publication.MaterialType;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.SqlDestination;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PublishConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.exposed.rest.StandaloneHttpServer;

public class PublicationServiceImplTestNG extends PropDataEngineFunctionalTestNGBase {

    private static final String PUBLICATION_NAME = "TestPublication2";
    private static final String CURRENT_VERSION = "version2";
    private final String SUBMITTER = this.getClass().getSimpleName();

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

    @Autowired
    private WorkflowProxy workflowProxy;

    private Publication publication;
    private PublicationRequest publicationRequest;

    private StandaloneHttpServer httpServer;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(this.getClass().getSimpleName());
        hdfsSourceEntityMgr.setCurrentVersion(source, CURRENT_VERSION);
        publicationRequest = new PublicationRequest();
        publicationRequest.setSubmitter(SUBMITTER);
        publicationRequest.setSourceVersion(CURRENT_VERSION);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        publicationEntityMgr.removePublication(PUBLICATION_NAME);
        httpServer.stop();
    }

    @BeforeMethod(groups = "functional")
    public void setupMethod() {
        publicationEntityMgr.removePublication(PUBLICATION_NAME);
        publication = getPublication();
    }

    @Test(groups = "functional", priority = 2)
    public void testPublish() {
        List<PublicationProgress> progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 0, "Should have zero progress at beginning");

        publicationService.kickoff(PUBLICATION_NAME, publicationRequest);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 1, "Should have a new progress");

        publicationService.kickoff(PUBLICATION_NAME, publicationRequest);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 1, "Should still have one progress");

        PublicationProgress progress1 = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                CURRENT_VERSION);
        publicationProgressService.update(progress1).retry().retry().retry()
                .status(ProgressStatus.FAILED).commit();
        publicationService.kickoff(PUBLICATION_NAME, publicationRequest);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 2, "Should have one more progress.");
        // test status of latest publication progress with required version
        ProgressStatus status = publicationService.findProgressAtVersion(PUBLICATION_NAME, CURRENT_VERSION);
        Assert.assertEquals(status, ProgressStatus.NEW);
    }

    @Test(groups = "functional", priority = 1)
    public void testScan() throws Exception {
        startWorkFlowServer(configuration -> {
            String json = configuration.getConfigRegistry().get(PublishConfiguration.class.getCanonicalName());
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode jsonNode = mapper.readTree(json).get("publication");
                Publication publication = mapper.treeToValue(jsonNode, Publication.class);
                PublicationConfiguration pubConfig = publication.getDestinationConfiguration();
                Assert.assertTrue(pubConfig instanceof PublishToSqlConfiguration);

                jsonNode = mapper.readTree(json).get("progress");
                PublicationProgress progress = mapper.treeToValue(jsonNode, PublicationProgress.class);
                Assert.assertNotNull(progress.getDestination());
                Assert.assertTrue(progress.getDestination() instanceof SqlDestination);
            } catch (IOException e) {
                Assert.fail("Failed to parse publication configuration.", e);
            }
        });
        publicationService.kickoff(PUBLICATION_NAME, publicationRequest);
        try {
            workflowProxy.setHostport("http://localhost:8234");
            publicationService.scan();
        } catch (Exception e) {
            Assert.fail("Error from workflow proxy.", e);
        }
    }

    private Publication getPublication() {
        Publication publication = new Publication();
        publication.setPublicationName(PUBLICATION_NAME);
        publication.setSourceName(source.getSourceName());
        publication.setNewJobMaxRetry(3);
        publication.setSchedularEnabled(true);
        publication.setMaterialType(MaterialType.INGESTION);
        publication.setPublicationType(Publication.PublicationType.SQL);

        PublishToSqlConfiguration configuration = new PublishToSqlConfiguration();
        configuration.setDefaultTableName("DefaultTable");
        configuration.setPublicationStrategy(PublicationConfiguration.PublicationStrategy.VERSIONED);
        publication.setDestinationConfiguration(configuration);

        return publicationEntityMgr.addPublication(publication);
    }

    private void startWorkFlowServer(PublicationWorkflowServlet.PayloadVerifier verifier) throws Exception {
        httpServer = new StandaloneHttpServer();
        httpServer.init(8234);
        httpServer.addServlet(new PublicationWorkflowServlet(verifier), "/workflowapi/workflows/");
        httpServer.start();
    }

}
