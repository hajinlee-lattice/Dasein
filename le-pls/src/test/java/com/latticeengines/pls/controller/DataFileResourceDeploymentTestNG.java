package com.latticeengines.pls.controller;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryParser;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class DataFileResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DataFileResourceDeploymentTestNG.class);
    private static final String UUID = "8195dcf1-0898-4ad3-b94d-0d0f806e979e";

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    private ModelSummaryParser modelSummaryParser;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        switchToSuperAdmin();
        modelSummaryParser = new ModelSummaryParser();

        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/" + mainTestTenant.getId());
        String dir = modelingServiceHdfsBaseDir + "/" + mainTestTenant.getId() + "/models/ANY_TABLE/" + UUID
                + "/1423547416066_0001/";
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/modelsummary-eloqua-token.json");

        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir + "/enhancements");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/diagnostics.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/metadata.avsc");

        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_model.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_readoutsample.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_scored.txt");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_explorer.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/rf_model.txt");
        String newDir = modelingServiceHdfsBaseDir + "/" + CustomerSpace.parse(mainTestTenant.getId())
                + "/data/ANY_TABLE/csv_files";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(),
                newDir + "/postMatchEventTable_allTraining-r-00000.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(),
                newDir + "/postMatchEventTable_allTest-r-00000.csv");
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, Thread.currentThread().getContextClassLoader().getResourceAsStream(
                "com/latticeengines/pls/functionalframework/trainingfile.csv"), newDir + "/trainingfile.csv");

        String content = FileUtils.readFileToString(new File(modelSummaryUrl.getFile()));
        content = content.replace("{uuid}", UUID);
        content = content.replace("{tenantId}", mainTestTenant.getId());
        ModelSummary summary = modelSummaryParser.parse(dir + "/enhancements/modelsummary.json", content);
        summary.setId("ms__" + UUID + "-model");
        summary.getModelSummaryConfiguration().setProvenanceProperty(ProvenancePropertyName.TrainingFilePath, newDir + "/trainingfile.csv");

        restTemplate.postForEntity(getRestAPIHostPort() + "/pls/modelsummaries", summary, ModelSummary.class);
        HdfsUtils.writeToFile(yarnConfiguration, dir + "/enhancements/modelsummary.json", JsonUtils.serialize(summary));
    }

    @AfterClass(groups = { "deployment" })
    public void teardown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/" + mainTestTenant.getId());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = { "deployment" }, dataProvider = "dataFileProvider")
    public void dataFileResource(String fileType, final String mimeType) {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        Assert.assertNotNull(response);
        Assert.assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);

        String modelId = map.get("Id");
        restTemplate.execute(getRestAPIHostPort() + "/pls/datafiles/" + fileType + "?modelId=" + modelId,
                HttpMethod.GET, new RequestCallback() {
                    @Override
                    public void doWithRequest(ClientHttpRequest request) throws IOException {
                    }
                }, new ResponseExtractor<Map<String, String>>() {
                    @Override
                    public Map<String, String> extractData(ClientHttpResponse response) throws IOException {
                        Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
                        HttpHeaders headers = response.getHeaders();
                        Assert.assertTrue(headers.containsKey("Content-Disposition"));
                        Assert.assertTrue(headers.containsKey("Content-Type"));
                        Assert.assertEquals(headers.getFirst("Content-Type"), mimeType);
                        List<String> lines = IOUtils.readLines(response.getBody());
                        Assert.assertTrue(lines.size() > 0);
                        /*
                        if (fileType.equals("trainingfilecsv")) {
                            Assert.assertTrue(lines.get(0).contains("Event"));
                        }*/
                        response.close();
                        return Collections.emptyMap();
                    }
                });
    }

    @DataProvider(name = "dataFileProvider")
    public static Object[][] getDataFileProvider() {
        return new Object[][] {
                { "modeljson", MediaType.APPLICATION_JSON }, //
                { "diagnosticsjson", MediaType.APPLICATION_JSON }, //
                { "metadataavsc", MediaType.APPLICATION_JSON }, //
                { "predictorcsv", "application/csv" }, //
                { "readoutcsv", "application/csv" }, //
                { "scorecsv", MediaType.TEXT_PLAIN }, //
                { "explorercsv", "application/csv" }, //
                { "rfmodelcsv", "application/csv" }, //
                { "postmatcheventtablecsv/training", MediaType.APPLICATION_OCTET_STREAM }, //
                { "postmatcheventtablecsv/test", MediaType.APPLICATION_OCTET_STREAM },
                { "trainingfilecsv", MediaType.APPLICATION_OCTET_STREAM }
        };
    }
}
