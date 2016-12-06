package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;

public class ModelRetrieverDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(ModelRetrieverDeploymentTestNG.class);

    @Autowired
    private ModelRetriever modelRetriever;

    @Test(groups = "deployment", enabled = true)
    public void testRetrieveModelArtifacts() throws Exception {
        tenant = setupTenantAndModelSummary(false);

        ScoringArtifacts artifacts = modelRetriever.getModelArtifacts(customerSpace, MODEL_ID);
        testArtifacts(artifacts);
        // Fetch the artifacts second time directly from the cache
        ScoringArtifacts cachedArtifacts = modelRetriever.getModelArtifacts(customerSpace, MODEL_ID);
        testArtifacts(cachedArtifacts);
    }

    private void testArtifacts(ScoringArtifacts artifacts) {
        Assert.assertNotNull(artifacts);
        String localModelJsonCacheDir = String.format(ModelRetrieverImpl.LOCAL_MODELJSON_CACHE_DIR,
                customerSpace.toString(), MODEL_ID);
        File modelArtifactsDir = new File(localModelJsonCacheDir + ModelRetrieverImpl.LOCAL_MODEL_ARTIFACT_CACHE_DIR);
        Assert.assertEquals(artifacts.getModelArtifactsDir(), modelArtifactsDir);
    }

    /**
     * Use this as a tool to download a model into test resources for running
     * scoring tests against.
     * 
     * @throws Exception
     */
    @Test(groups = "deployment", enabled = false)
    public void downloadModelToLocal() throws Exception {
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
