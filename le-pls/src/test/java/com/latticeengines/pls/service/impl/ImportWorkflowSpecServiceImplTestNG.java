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
import com.latticeengines.pls.util.ImportWorkflowUtilsTestNG;

public class ImportWorkflowSpecServiceImplTestNG extends PlsFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowSpecServiceImplTestNG.class);

    private static String testSpecFileName =
            "com/latticeengines/pls/service/impl/importworkflowspecservice/test-contact-spec.json";

    private static String expectedTableFileName =
            "com/latticeengines/pls/service/impl/importworkflowspecservice/expected-table.json";


    @Autowired
    private ImportWorkflowSpecServiceImpl importWorkflowSpecService;

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
            actualTestSpec = importWorkflowSpecService.loadSpecFromS3("test", "contact");
        } catch (Exception e) {
            log.error("Loading Spec from S3 failed with error:", e);
            throw e;
        }
        Assert.assertNotNull(actualTestSpec);
        log.error("Actual import workflow spec is:\n" + JsonUtils.pprint(actualTestSpec));

        // TODO(jwinter): Figure out the proper way to compare JSON objects for test.
        Assert.assertEquals(JsonUtils.serialize(actualTestSpec), JsonUtils.serialize(expectedTestSpec));

        /*
        Assert.assertEquals(actualTestSpec.getSystemType(), expectedTestSpec.getSystemType());
        Assert.assertEquals(actualTestSpec.getSystemObject(), expectedTestSpec.getSystemObject());
        */
    }

    @Test(groups = "functional")
    public void testTableFromSpec() throws Exception {
        ImportWorkflowSpec testSpec = null;
        try {
            testSpec = importWorkflowSpecService.loadSpecFromS3("test", "contact");
        } catch (Exception e) {
            log.error("Loading Spec from S3 failed with error:", e);
            throw e;
        }

        Table actualTable = importWorkflowSpecService.tableFromSpec(testSpec);
        log.error("Actual table generated from import workflow spec is:\n" + JsonUtils.pprint(actualTable));

        Table expectedTable = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(expectedTableFileName, Table.class);
        log.error("Expected table generated from import workflow spec is:\n" + JsonUtils.pprint(expectedTable));

        Assert.assertEquals(JsonUtils.serialize(actualTable), JsonUtils.serialize(expectedTable));
    }

}
