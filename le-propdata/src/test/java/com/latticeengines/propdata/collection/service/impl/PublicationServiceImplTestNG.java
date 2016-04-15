package com.latticeengines.propdata.collection.service.impl;

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
import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.propdata.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.propdata.publication.PublicationRequest;
import com.latticeengines.domain.exposed.propdata.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.propdata.publication.SqlDestination;
import com.latticeengines.propdata.collection.entitymgr.PublicationEntityMgr;
import com.latticeengines.propdata.collection.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PublicationProgressService;
import com.latticeengines.propdata.collection.service.PublicationService;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;
import com.latticeengines.propdata.collection.testframework.PublicationWorkflowServlet;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.source.impl.BuiltWithPivoted;
import com.latticeengines.propdata.workflow.collection.PublicationWorkflowConfiguration;
import com.latticeengines.propdata.workflow.collection.steps.PublishConfiguration;
import com.latticeengines.testframework.rest.StandaloneHttpServer;

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
    private PublicationRequest publicationRequest;

    private StandaloneHttpServer httpServer;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(POD_ID);
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

    @Test(groups = "functional")
    public void testPublish() {
        List<PublicationProgress> progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 0, "Should have zero progress at beginning");

        publicationService.publish(PUBLICATION_NAME, publicationRequest, POD_ID);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 1, "Should have a new progress");

        publicationService.publish(PUBLICATION_NAME, publicationRequest, POD_ID);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 1, "Should still have one progress");

        PublicationProgress progress1 = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                CURRENT_VERSION);
        publicationProgressService.update(progress1).retry().retry().retry()
                .status(PublicationProgress.Status.FAILED).commit();
        publicationService.publish(PUBLICATION_NAME, publicationRequest, POD_ID);
        progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertEquals(progresses.size(), 2, "Should have one more progress.");
    }

    @Test(groups = "functional")
    public void testScan() throws Exception {
        startWorkFlowServer(new PublicationWorkflowServlet.PayloadVerifier() {
            @Override
            public void verify(PublicationWorkflowConfiguration configuration) {
                String json = configuration.getConfigRegistry().get(PublishConfiguration.class.getCanonicalName());
                ObjectMapper mapper = new ObjectMapper();
                try {
                    JsonNode jsonNode = mapper.readTree(json).get("pub_config");
                    PublicationConfiguration pubConfig = mapper.treeToValue(jsonNode, PublicationConfiguration.class);
                    Assert.assertTrue(pubConfig instanceof PublishToSqlConfiguration);
                    Assert.assertNotNull(pubConfig.getDestination());
                    Assert.assertTrue(pubConfig.getDestination() instanceof SqlDestination);
                } catch (IOException e) {
                    Assert.fail("Failed to parse publication configuration.", e);
                }
            }
        });
        publicationService.publish(PUBLICATION_NAME, publicationRequest, POD_ID);
        publicationService.scan(POD_ID);
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

    private void startWorkFlowServer(PublicationWorkflowServlet.PayloadVerifier verifier) throws Exception {
        httpServer = new StandaloneHttpServer();
        httpServer.init(8080);
        httpServer.addServlet(new PublicationWorkflowServlet(verifier), "/workflowapi/workflows/");
        httpServer.start();
    }

}
