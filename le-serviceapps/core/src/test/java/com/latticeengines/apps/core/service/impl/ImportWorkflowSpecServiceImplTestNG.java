package com.latticeengines.apps.core.service.impl;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.service.ImportWorkflowSpecService;
import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
public class ImportWorkflowSpecServiceImplTestNG extends ServiceAppsFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowSpecServiceImplTestNG.class);

    private static String testSpecFileName =
            "com/latticeengines/apps/core/service/impl/importworkflowspecservice/specfunctest-contacts-spec.json";

    private static String expectedTableFileName =
            "com/latticeengines/apps/core/service/impl/importworkflowspecservice/expected-spec-table.json";

    @Inject
    private S3Service s3Service;

    @Inject
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

    @Test(groups = "functional")
    public void loadSpecWithSameObjectExcludeTypeFromS3() throws Exception {
        String systemType = "specfunctest", systemObject = "contacts";
        List<ImportWorkflowSpec> specList = null;
        try {
            specList = importWorkflowSpecService.loadSpecsByTypeAndObject(null, systemObject, systemType);

        } catch (Exception e) {
            log.error("Loading Spec from S3 failed with error:", e);
            throw e;
        }

        Assert.assertNotNull(specList);
        Assert.assertTrue(specList.size() > 0);

        for(ImportWorkflowSpec spec : specList) {
            Assert.assertEquals(systemObject, spec.getSystemObject().toLowerCase());
        }
        ImportWorkflowSpec spec =
                specList.stream().filter(workflowSpec -> systemType.equals(workflowSpec.getSystemType().toLowerCase())).findFirst().orElse(null);
        Assert.assertNull(spec);
    }

    @Test(groups = "functional")
    public void uploadSpecToS3() throws Exception {
        String fakeType = "fakeType", fakeObject = "fakeObject";
        ImportWorkflowSpec fakeSpec = new ImportWorkflowSpec();
        fakeSpec.setSystemName("fakeName");
        fakeSpec.setSystemType(fakeType);
        fakeSpec.setSystemName(fakeObject);
        importWorkflowSpecService.addSpecToS3(fakeType, fakeObject, fakeSpec);
        ImportWorkflowSpec importSpec = importWorkflowSpecService.loadSpecFromS3(fakeType, fakeObject);
        Assert.assertNotNull(importSpec);
        importWorkflowSpecService.deleteSpecFromS3(fakeType, fakeObject);
        try {
            importSpec = importWorkflowSpecService.loadSpecFromS3(fakeType, fakeObject);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOException);
            importSpec = null;
        }
        Assert.assertNull(importSpec);
    }
}
