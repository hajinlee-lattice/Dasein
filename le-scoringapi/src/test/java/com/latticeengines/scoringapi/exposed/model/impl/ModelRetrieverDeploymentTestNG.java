package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;

public class ModelRetrieverDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(ModelRetrieverDeploymentTestNG.class);

    @Autowired
    private ModelRetriever modelRetriever;

    @Autowired
    private Configuration yarnConfiguration;

    @Test(groups = "deployment", enabled = true)
    public void testRetrieveModelArtifacts() throws Exception {
        tenant = setupTenantAndModelSummary(false);
        ScoringArtifacts artifacts = modelRetriever.getModelArtifacts(customerSpace, MODEL_ID);
        Assert.notNull(artifacts);
    }

    /**
     * Use this as a tool to download a model into test resources for running scoring tests against.
     * @throws Exception
     */
    @Test(groups = "deployment", enabled = false)
    public void downloadModelToLocal() throws Exception {
        SSLUtils.turnOffSslChecking();
        String localPathToPersist = "/users/bnguyen/dev/ledp/le-scoringapi/src/test/resources/com/latticeengines/scoringapi/model/NPE/";
        File file = new File(localPathToPersist);
        file.mkdirs();
        String tenantId = "Lattice_LP3.Lattice_LP3.Production";
        String modelId = "ms__d2e04d4a-8c67-4a01-8dc5-f2d85e7751de-LP2-Clon";

        log.info(String.format("Downloading model-score artifacts from HDFS for tenant:%s model%s to %s", tenantId,
                modelId, localPathToPersist));
        modelRetriever.setLocalPathToPersist(localPathToPersist);
        modelRetriever.retrieveModelArtifactsFromHdfs(CustomerSpace.parse(tenantId), modelId);

        log.info("Done");
    }

}
