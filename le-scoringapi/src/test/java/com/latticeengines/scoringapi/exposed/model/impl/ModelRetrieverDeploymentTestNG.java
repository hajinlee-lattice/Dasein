package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;
import com.latticeengines.scoringapi.functionalframework.ScoringApiTestUtils;

public class ModelRetrieverDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ModelRetrieverDeploymentTestNG.class);

    @Autowired
    private ModelRetrieverImpl modelRetriever;

    @Test(groups = "deployment", enabled = true)
    public void testRetrieveModelArtifacts() throws Exception {
        long sizeOfPmmlFile = FileUtils.sizeOf(new File(Thread.currentThread().getContextClassLoader()
                .getResource(LOCAL_MODEL_PATH + ModelJsonTypeHandler.PMML_FILENAME).getPath()));
        log.info(String.format("Size of the pmml file is %d", sizeOfPmmlFile));
        long memBeforeScoreArtifactCreation = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        log.info(String.format("Before generating the score artifact, the free memory is %d",
                memBeforeScoreArtifactCreation));
        ScoringArtifacts artifacts = modelRetriever.getModelArtifacts(customerSpace, MODEL_ID);
        Assert.assertEquals(sizeOfPmmlFile,
                modelRetriever.getSizeOfPMMLFile(customerSpace, artifacts.getModelSummary()));
        long memAfterScoreArtifactCreation = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        log.info(String.format("After generating the score artifact, the free memory is %d",
                memAfterScoreArtifactCreation));
        log.info(String.format("Memory change due to score artifact is %d",
                (memAfterScoreArtifactCreation - memBeforeScoreArtifactCreation)));
        double factor = (double) (memAfterScoreArtifactCreation - memBeforeScoreArtifactCreation) / (sizeOfPmmlFile);
        log.info(String.format("factor is %f", factor));
        testArtifacts(artifacts);
        // Fetch the artifacts second time directly from the cache
        ScoringArtifacts cachedArtifacts = modelRetriever.getModelArtifacts(customerSpace, MODEL_ID);
        testArtifacts(cachedArtifacts);
    }

    private void testArtifacts(ScoringArtifacts artifacts) throws IOException {
        Assert.assertNotNull(artifacts);
        Assert.assertNotNull(artifacts.getModelSummary());
        Assert.assertNotNull(artifacts.getBucketMetadataList());
        List<BucketMetadata> expectedBucketMetadataList = ScoringApiTestUtils.generateDefaultBucketMetadataList();
        Assert.assertEquals(artifacts.getBucketMetadataList().size(), expectedBucketMetadataList.size());
        String localModelJsonCacheDir = String.format(modelRetriever.getLocalModelJsonCacheDirProperty(),
                modelRetriever.getLocalModelJsonCacheDirIdentifier(), customerSpace.toString(), MODEL_ID);
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
