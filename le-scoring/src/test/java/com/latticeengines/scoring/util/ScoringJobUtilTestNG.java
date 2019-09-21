package com.latticeengines.scoring.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

public class ScoringJobUtilTestNG extends ScoringFunctionalTestNGBase {

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    private String tenant = CustomerSpace.parse(this.getClass().getSimpleName()).toString();

    private String dir;

    @BeforeMethod(groups = "functional", enabled = false)
    public void beforeMethod() throws Exception {
        URL modelSummaryUrl = ClassLoader.getSystemResource("com/latticeengines/scoring/data/part-m-00000.avro"); //
        HdfsUtils.mkdir(yarnConfiguration, dir);
        String filePath = dir + "/part-m-00000.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), filePath);
    }

    @BeforeClass(groups = "functional", enabled = false)
    public void setup() throws Exception {
        dir = customerBaseDir + "/test_customer/scoring/data/some_table";
        HdfsUtils.rmdir(yarnConfiguration, dir);
    }

    @Test(groups = "functional", enabled = false)
    public void findModelUrlsToLocalize() throws Exception {
        List<String> modelGuidsInHdfs = Arrays.<String> asList(new String[] { "c412ea67-6c5b-472e-91ef-d7cfd375c24d",
                "02ed95bf-4bad-44da-9274-7de877d5612b", "0fc8614c-85ad-405c-8b1d-ff38f94ec741",
                "231c3244-4770-424d-aa12-f9d701623876" });
        List<String> modelFilePaths = new ArrayList<String>();
        int numModelsInHdfs = 4;
        for (int i = 0; i < numModelsInHdfs; i++) {
            String modelDir = createModelDirPath(tenant, modelGuidsInHdfs.get(i), i);
            HdfsUtils.mkdir(yarnConfiguration, modelDir);
            String modelFilePath = modelDir + "/model" + i + ".json";
            HdfsUtils.writeToFile(yarnConfiguration, modelFilePath, String.valueOf(i));
            modelFilePaths.add(modelFilePath);
        }
        List<String> modelGuidsFromTable = Arrays.<String> asList(new String[] {
                constructModelGuid("c412ea67-6c5b-472e-91ef-d7cfd375c24d"),
                constructModelGuid("0fc8614c-85ad-405c-8b1d-ff38f94ec741") });

        assertEquals(modelFilePaths.size(), 4);
        assertEquals(modelGuidsFromTable.size(), 2);
        List<String> modelUrls = ScoringJobUtil.findModelUrlsToLocalize(yarnConfiguration, tenant, modelGuidsFromTable,
                modelFilePaths, false);

        assertEquals(modelUrls.size(), modelGuidsFromTable.size());
        List<String> retrievedUuids = new ArrayList<>();
        for (String url : modelUrls) {
            String[] tokens = url.split("#");
            assertEquals(tokens[1], UuidUtils.parseUuid(tokens[0]));
            retrievedUuids.add(tokens[1]);
        }

        for (String modelGuid : modelGuidsFromTable) {
            String uuid = UuidUtils.extractUuid(modelGuid);
            assertTrue(retrievedUuids.contains(uuid));
        }
        System.out.println(modelUrls);

        String nonExistUuid = "bddc1633-2305-4824-9a15-9efb2e430279";
        modelGuidsFromTable = Arrays.<String> asList(new String[] {
                constructModelGuid("c412ea67-6c5b-472e-91ef-d7cfd375c24d"),
                constructModelGuid("0fc8614c-85ad-405c-8b1d-ff38f94ec741"), constructModelGuid(nonExistUuid) });
        try {
            modelUrls = ScoringJobUtil.findModelUrlsToLocalize(yarnConfiguration, tenant, modelGuidsFromTable,
                    modelFilePaths, false);
        } catch (LedpException e) {
            assertEquals(e.getCode(), LedpCode.LEDP_18007);
            assertTrue(e.getMessage().contains(nonExistUuid));
        }
        for (int i = 0; i < numModelsInHdfs; i++) {
            HdfsUtils.rmdir(yarnConfiguration, customerBaseDir + "/" + tenant + "/models/some_event_table" + i);
        }
    }

    private String createModelDirPath(String tenant, String uuid, int modelNum) {
        return customerBaseDir + "/" + tenant + "/models/some_event_table" + modelNum + "/" + uuid
                + "/1445290697489_009" + modelNum;
    }

    private String constructModelGuid(String uuid) {
        return "ms__" + uuid + "-PLSModel";
    }

    @Test(groups = "functional", enabled = false)
    public void testGenerateDataTypeSchema() throws JsonParseException, JsonMappingException, IOException, Exception {
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(dir + "/part-m-00000.avro"));
        JsonNode dataType = ScoringJobUtil.generateDataTypeSchema(schema);
        assertEquals(dataType.get("LeadID").asText(), "1");
        assertEquals(dataType.get("Probability").asText(), "0");
        assertEquals(dataType.get("RawScore").asText(), "0");
        assertEquals(dataType.get("Play_Display_Name").asText(), "1");
        assertEquals(dataType.get("Bucket_Display_Name").asText(), "1");
        assertEquals(dataType.get("Score").asText(), "0");
        assertEquals(dataType.get("Percentile").asText(), "0");
        assertEquals(dataType.get("Lift").asText(), "0");
    }
}
