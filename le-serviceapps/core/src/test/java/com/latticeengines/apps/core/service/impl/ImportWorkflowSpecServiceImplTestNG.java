package com.latticeengines.apps.core.service.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.service.ImportWorkflowSpecService;
import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;

public class ImportWorkflowSpecServiceImplTestNG extends ServiceAppsFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowSpecServiceImplTestNG.class);

    private static String testSpecFileName =
            "com/latticeengines/pls/service/impl/importworkflowspecservice/specfunctest-contacts-spec.json";

    private static String expectedTableFileName =
            "com/latticeengines/pls/service/impl/importworkflowspecservice/expected-spec-table.json";


    @Autowired
    private ImportWorkflowSpecService importWorkflowSpecService;

    @Test(groups = "functional")
    public void testLoadSpecFromS3() throws IOException {
        ImportWorkflowSpec expectedTestSpec = JsonUtils.pojoFromJsonResourceFile(testSpecFileName,
                ImportWorkflowSpec.class);

        log.error("Expected import workflow spec is:\n" + JsonUtils.pprint(expectedTestSpec));

        ImportWorkflowSpec actualTestSpec = null;
        try {
            actualTestSpec = importWorkflowSpecService.loadSpecFromS3("specfunctest", "contacts");
        } catch (IOException e) {
            throw new IOException("Test of Loading Spec from S3 failed with error:", e);
        }
        Assert.assertNotNull(actualTestSpec);
        log.error("Actual import workflow spec is:\n" + JsonUtils.pprint(actualTestSpec));

        Assert.assertEquals(actualTestSpec, expectedTestSpec);

        // TODO(jwinter): Figure out the proper way to compare JSON objects for test.
        // Below are some alternative equality checks.
        //Assert.assertEquals(JsonUtils.serialize(actualTestSpec), JsonUtils.serialize(expectedTestSpec));
        //ObjectMapper mapper = new ObjectMapper();
        //Assert.assertTrue(mapper.valueToTree(actualTestSpec).equals(mapper.valueToTree(expectedTestSpec)));
    }

    @Test(groups = "functional")
    public void testTableFromSpec() throws IOException {
        ImportWorkflowSpec testSpec = null;
        try {
            testSpec = importWorkflowSpecService.loadSpecFromS3("specfunctest", "contacts");
        } catch (IOException e) {
            throw new IOException("Loading Spec from S3 before creating table failed with error:", e);
        }

        Table actualTable = importWorkflowSpecService.tableFromRecord(null, true, testSpec);
        log.error("Actual table generated from import workflow spec is:\n" + JsonUtils.pprint(actualTable));

        Table expectedTable = JsonUtils.pojoFromJsonResourceFile(expectedTableFileName, Table.class);
        log.error("Expected table generated from import workflow spec is:\n" + JsonUtils.pprint(expectedTable));

        Assert.assertEquals(JsonUtils.serialize(actualTable), JsonUtils.serialize(expectedTable));
        // TODO(jwinter): Figure out the proper way to compare JSON objects for test.
        //ObjectMapper mapper = new ObjectMapper();
        //Assert.assertTrue(mapper.valueToTree(actualTable).equals(mapper.valueToTree(expectedTable)));
    }

}
