package com.latticeengines.propdata.api.controller;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.propdata.publication.PublicationRequest;
import com.latticeengines.domain.exposed.propdata.publication.PublishToSqlConfiguration;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.source.impl.BuiltWithPivoted;
import com.latticeengines.propdata.engine.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.propdata.engine.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.proxy.exposed.propdata.PublicationProxy;

@Component
public class PublicationResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    public final String POD_ID = this.getClass().getSimpleName();
    public static final String PUBLICATION_NAME = "TestPublication";
    public static final String CURRENT_VERSION = "version1";
    public final String SUBMITTER = this.getClass().getSimpleName();

    @Autowired
    private PublicationEntityMgr publicationEntityMgr;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PublicationProgressEntityMgr progressEntityMgr;

    @Autowired
    private BuiltWithPivoted source;

    @Autowired
    private PublicationProxy publicationProxy;

    @AfterClass(groups = "deployment")
    public void teardown() throws Exception {
        publicationEntityMgr.removePublication(PUBLICATION_NAME);
    }

    @Test(groups = "deployment")
    public void testPublish() {
        prepareCleanPod(POD_ID);
        uploadSourceAtVersion(source, CURRENT_VERSION);
        hdfsSourceEntityMgr.setCurrentVersion(source, CURRENT_VERSION);
        PublicationRequest publicationRequest = new PublicationRequest();
        publicationRequest.setSubmitter(SUBMITTER);
        publicationRequest.setSourceVersion(CURRENT_VERSION);

        publicationEntityMgr.removePublication(PUBLICATION_NAME);
        Publication publication = registerPublication();

        List<PublicationProgress> progressList = publicationProxy.scan(POD_ID);
        Assert.assertTrue(progressList.size() >= 1, "Should trigger at least one progress.");
        PublicationProgress progress = progressList.get(0);

        ApplicationId appId = ConverterUtils.toApplicationId(progress.getApplicationId());
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId, 3600);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        List<PublicationProgress> progresses = progressEntityMgr.findAllForPublication(publication);
        Assert.assertTrue(progresses.size() >= 1, "Should have at least one progress for the testing publication");
        Assert.assertEquals(progresses.get(0).getStatus(), ProgressStatus.FINISHED,
                "The final status of the progress is not " + ProgressStatus.FINISHED + ", but "
                        + progresses.get(0).getStatus());
    }

    private Publication registerPublication() {
        Publication publication = new Publication();
        publication.setPublicationName(PUBLICATION_NAME);
        publication.setSourceName(source.getSourceName());
        publication.setNewJobMaxRetry(3);
        publication.setSchedularEnabled(true);
        publication.setPublicationType(Publication.PublicationType.SQL);

        PublishToSqlConfiguration configuration = new PublishToSqlConfiguration();
        configuration.setAlias(PublishToSqlConfiguration.Alias.TestDB);
        configuration.setDefaultTableName(source.getSourceName());
        configuration.setPublicationStrategy(PublishToSqlConfiguration.PublicationStrategy.VERSIONED);
        publication.setDestinationConfiguration(configuration);

        return publicationEntityMgr.addPublication(publication);
    }

}
