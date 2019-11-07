package com.latticeengines.pls.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.ImportWorkflowSpecService;
import com.latticeengines.pls.util.ImportWorkflowUtilsTestNG;

public class ImportWorkflowSpecServiceImplTestNG extends PlsFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowSpecServiceImplTestNG.class);

    private static String testSpecFileName =
            "com/latticeengines/pls/service/impl/importworkflowspecservice/specfunctest-contacts-spec.json";

    private static String expectedTableFileName =
            "com/latticeengines/pls/service/impl/importworkflowspecservice/expected-spec-table.json";


    @Autowired
    private ImportWorkflowSpecService importWorkflowSpecService;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }

    @Test(groups = "functional")
    public void testLoadSpecFromS3() throws Exception {
        ImportWorkflowSpec expectedTestSpec = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(testSpecFileName,
                ImportWorkflowSpec.class);

        log.error("Expected import workflow spec is:\n" + JsonUtils.pprint(expectedTestSpec));

        ImportWorkflowSpec actualTestSpec = null;
        try {
            actualTestSpec = importWorkflowSpecService.loadSpecFromS3("specfunctest", "contacts");
        } catch (Exception e) {
            log.error("Loading Spec from S3 failed with error:", e);
            throw e;
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
    public void testTableFromSpec() throws Exception {
        ImportWorkflowSpec testSpec = null;
        try {
            testSpec = importWorkflowSpecService.loadSpecFromS3("specfunctest", "contacts");
        } catch (Exception e) {
            log.error("Loading Spec from S3 failed with error:", e);
            throw e;
        }

        Table actualTable = importWorkflowSpecService.tableFromSpec(testSpec);
        log.error("Actual table generated from import workflow spec is:\n" + JsonUtils.pprint(actualTable));

        Table expectedTable = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(expectedTableFileName, Table.class);
        log.error("Expected table generated from import workflow spec is:\n" + JsonUtils.pprint(expectedTable));

        Assert.assertEquals(JsonUtils.serialize(actualTable), JsonUtils.serialize(expectedTable));
        // TODO(jwinter): Figure out the proper way to compare JSON objects for test.
        //ObjectMapper mapper = new ObjectMapper();
        //Assert.assertTrue(mapper.valueToTree(actualTable).equals(mapper.valueToTree(expectedTable)));
    }

}
