package com.latticeengines.datacloudapi.api.controller;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ACCOUNT_MASTER;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.datacloud.etl.publication.service.impl.DynamoPublishService;
import com.latticeengines.datacloudapi.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.publication.DynamoDestination;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToDynamoConfiguration;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.proxy.exposed.datacloudapi.PublicationProxy;
import com.latticeengines.yarn.exposed.service.JobService;

/**
 * dpltc deploy -a workflowapi,datacloudapi,modeling,eai,sqoop
 */
@Component
public class PublicationResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    private static final String POD_ID = PublicationResourceDeploymentTestNG.class.getSimpleName();
    private static final String CURRENT_VERSION = HdfsPathBuilder.dateFormat.format(new Date());
    private static final String SUBMITTER = DataCloudConstants.SERVICE_TENANT;

    private static final String SQL_SOURCE = "BuiltWithPivoted";

    private static final String DYNAMO_RECORD_TYPE = "LatticeAccount";

    @Inject
    private PublicationEntityMgr publicationEntityMgr;

    @Inject
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    private PublicationProgressEntityMgr progressEntityMgr;

    @Inject
    private PublicationProxy publicationProxy;

    @Inject
    private JobService jobService;

    @Inject
    private DynamoService dynamoService;

    @Value("${common.le.stack}")
    private String leStack;

    private ThreadLocal<String> publicationName = new ThreadLocal<>();

    @AfterMethod(groups = "deployment")
    public void teardown() {
        publicationEntityMgr.removePublication(publicationName.get());
        String tableName = DynamoPublishService.convertToFabricStoreName(DYNAMO_RECORD_TYPE + "_" + leStack);
        dynamoService.deleteTable(tableName);
    }

    @Test(groups = "deployment")
    public void testPublishDynamo() {
        publicationName.set("Test" + ACCOUNT_MASTER + "Publication");

        prepareCleanPod(POD_ID);
        uploadSourceAtVersion(ACCOUNT_MASTER, CURRENT_VERSION);
        hdfsSourceEntityMgr.setCurrentVersion(ACCOUNT_MASTER, CURRENT_VERSION);

        PublicationRequest publicationRequest = new PublicationRequest();
        publicationRequest.setSubmitter(SUBMITTER);
        publicationRequest.setSourceVersion(CURRENT_VERSION);

        DynamoDestination destination = new DynamoDestination();
        destination.setVersion("_" + leStack);
        publicationRequest.setDestination(destination);

        publicationEntityMgr.removePublication(publicationName.get());
        Publication publication = registerDynamoPublication(publicationName.get());

        AppSubmission appSubmission = publicationProxy.publish(publicationName.get(), publicationRequest, POD_ID)
                .getAppSubmissions();
        Assert.assertNotNull(appSubmission);

        JobStatus jobStatus = jobService.waitFinalJobStatus(appSubmission.getApplicationIds().get(0), 3600);
        Assert.assertEquals(jobStatus.getStatus(), FinalApplicationStatus.SUCCEEDED);

        List<PublicationProgress> progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertTrue(progresses.size() >= 1, "Should have at least one progress for the testing publication");
        Assert.assertEquals(progresses.get(0).getStatus(), ProgressStatus.FINISHED,
                "The final status of the progress is not " + ProgressStatus.FINISHED + ", but "
                        + progresses.get(0).getStatus());
    }

    private Publication registerDynamoPublication(String publicationName) {
        Publication publication = new Publication();
        publication.setPublicationName(publicationName);
        publication.setSourceName(ACCOUNT_MASTER);
        publication.setNewJobMaxRetry(1);
        publication.setPublicationType(Publication.PublicationType.DYNAMO);
        publication.setMaterialType(Publication.MaterialType.SOURCE);


        PublishToDynamoConfiguration configuration = new PublishToDynamoConfiguration();
        configuration.setEntityClass(LatticeAccount.class.getCanonicalName());
        configuration.setRecordType(DYNAMO_RECORD_TYPE);
        configuration.setPublicationStrategy(PublicationConfiguration.PublicationStrategy.REPLACE);
        configuration.setAlias(PublishToDynamoConfiguration.Alias.QA);

        configuration.setLoadingReadCapacity(5L);
        configuration.setLoadingWriteCapacity(100L);
        configuration.setRuntimeReadCapacity(100L);
        configuration.setRuntimeWriteCapacity(5L);
        publication.setDestinationConfiguration(configuration);

        return publicationEntityMgr.addPublication(publication);
    }

}
