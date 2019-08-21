package com.latticeengines.pls.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;

public class ImportWorkflowUtilsTestNG extends PlsFunctionalTestNGBase {
    private static Logger log = LoggerFactory.getLogger(ImportWorkflowUtilsTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    String csvHdfsPath = "/tmp/test_import_workflow";

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        String path = ClassLoader
                .getSystemResource("com/latticeengines/pls/util/test-contact-import-file.csv").getPath();

        HdfsUtils.rmdir(yarnConfiguration, csvHdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, path, csvHdfsPath);
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, csvHdfsPath);
    }


    @Test(groups = "functional")
    public void testCreateFieldDefinitionsRecordFromSpecAndTable_noExistingTemplate() throws IOException {
        // Load CSV containing imported Contact data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(csvHdfsPath, yarnConfiguration, null);

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-contact-spec.json",
                ImportWorkflowSpec.class);

        // Generate actual fetch response based on Contact import CSV and Spec.
        FieldDefinitionsRecord actualResponse = ImportWorkflowUtils.createFieldDefinitionsRecordFromSpecAndTable(
                importWorkflowSpec, null, resolver);

        //log.error("Output (no existing template) fieldDefinitionsRecord is:\n" + JsonUtils.pprint(actualResponse));

        FieldDefinitionsRecord expectedResponse = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-response-no-existing-template.json",
                FieldDefinitionsRecord.class);

        ObjectMapper mapper = new ObjectMapper();
        Assert.assertTrue(mapper.valueToTree(actualResponse).equals(mapper.valueToTree(expectedResponse)));

        Assert.assertEquals(actualResponse, expectedResponse,
                "Actual Response:\n" + JsonUtils.pprint(actualResponse) + "\nvs\n\nExpected Response:\n" +
                        JsonUtils.pprint(expectedResponse));
    }

    @Test(groups = "functional")
    public void testCreateFieldDefinitionsRecordFromSpecAndTable_withExistingTemplate() throws IOException {
        // Load CSV containing imported Contact data for this test from resource file into HDFS.
        MetadataResolver resolver = new MetadataResolver(csvHdfsPath, yarnConfiguration, null);

        // Generate Spec Java class from resource file.
        ImportWorkflowSpec importWorkflowSpec = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-contact-spec.json",
                ImportWorkflowSpec.class);

        log.error("Import Workflow Spec is:\n" + JsonUtils.pprint(importWorkflowSpec));

        // Generate Table containing existing template from resource file.
        Table existingTemplateTable = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-existing-contact-template.json", Table.class);

        log.error("Existing Table is:\n" + JsonUtils.pprint(existingTemplateTable));

        // Generate actual fetch response based on Contact import CSV and Spec.
        FieldDefinitionsRecord actualResponse = ImportWorkflowUtils.createFieldDefinitionsRecordFromSpecAndTable(
                importWorkflowSpec, existingTemplateTable, resolver);

        //log.error("Output (existing template) fieldDefinitionsRecord is:\n" + JsonUtils.pprint(actualResponse));

        FieldDefinitionsRecord expectedResponse = pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-response-existing-template.json",
                FieldDefinitionsRecord.class);

        ObjectMapper mapper = new ObjectMapper();
        Assert.assertTrue(mapper.valueToTree(actualResponse).equals(mapper.valueToTree(expectedResponse)));

        Assert.assertEquals(actualResponse, expectedResponse,
                "Actual Response:\n" + JsonUtils.pprint(actualResponse) + "\nvs\n\nExpected Response:\n" +
                        JsonUtils.pprint(expectedResponse));
    }

    // resourceJsonFileRelativePath should start "com/latticeengines/...".
    public static <T> T pojoFromJsonResourceFile(String resourceJsonFileRelativePath, Class<T> clazz) throws
            IOException {
        T pojo = null;
        try {
            InputStream jsonInputStream = ClassLoader.getSystemResourceAsStream(resourceJsonFileRelativePath);
            if (jsonInputStream == null) {
                throw new IOException("Failed to convert resource file " + resourceJsonFileRelativePath +
                        " to InputStream.  Please check path");
            }
            pojo = JsonUtils.deserialize(jsonInputStream, clazz);
            if (pojo == null) {
                String jsonString = IOUtils.toString(jsonInputStream, Charset.defaultCharset());
                throw new IOException("POJO was null. Failed to deserialize InputStream containing string: " +
                        jsonString);
            }
        } catch (IOException e1) {
            log.error("File to POJO conversion failed for resource file " + resourceJsonFileRelativePath +
                    " with error: ", e1);
            throw e1;
        } catch (IllegalStateException e2) {
            log.error("File to POJO conversion failed for resource file " + resourceJsonFileRelativePath +
                    " with error: ", e2);
            throw e2;
        }
        return pojo;
    }


}
