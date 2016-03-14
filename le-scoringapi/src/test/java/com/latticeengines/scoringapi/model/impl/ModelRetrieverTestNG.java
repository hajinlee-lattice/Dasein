package com.latticeengines.scoringapi.model.impl;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.scoringapi.functionalframework.ScoringApiFunctionalTestNGBase;
import com.latticeengines.scoringapi.model.ModelRetriever;

public class ModelRetrieverTestNG extends ScoringApiFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(ModelRetrieverTestNG.class);

    @Autowired
    private ModelRetriever modelRetriever;

    /**
     * Use this as a tool to download a model into test resources for running scoring tests against.
     * @throws Exception
     */
    @Test(groups = "functional", enabled = false)
    public void downloadModelToLocal() throws Exception {
        String localPathToPersist = "/users/bnguyen/dev/ledp/le-scoringapi/src/test/resources/com/latticeengines/scoringapi/model/somemodel/";
        File file = new File(localPathToPersist);
        boolean result = file.mkdirs();
        String tenantId = "DevelopTestPLSTenant1.DevelopTestPLSTenant1.Production";
        String modelId = "ms__8ba99b36-c222-4f93-ab8a-6dcc11ce45e9-allRows_";

        log.info(String.format("Downloading model-score artifacts from HDFS for tenant:%s model%s to %s", tenantId,
                modelId, localPathToPersist));
        modelRetriever.setLocalPathToPersist(localPathToPersist);
        modelRetriever.retrieveModelArtifactsFromHdfs(CustomerSpace.parse(tenantId), modelId);

        log.info("Done");
    }
}
