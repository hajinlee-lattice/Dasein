package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.exposed.service.impl.ModelingServiceTestUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.functionalframework.EmptyResponseServlet;
import com.latticeengines.dataplatform.functionalframework.StandaloneHttpServer;
import com.latticeengines.dataplatform.functionalframework.VisiDBMetadataErrorResponseServlet;
import com.latticeengines.dataplatform.functionalframework.VisiDBMetadataServlet;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@SuppressWarnings("unused")
public class ModelStepRetrieveMetadataProcessorImplTestNG extends DataPlatformFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(ModelStepRetrieveMetadataProcessorImplTestNG.class);

    @Autowired
    private ModelStepRetrieveMetadataProcessorImpl modelStepRetrieveMetadataProcessor;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    private StandaloneHttpServer httpServer;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path("/user/s-analytics/customers/Nutanix"), true);
        httpServer = new StandaloneHttpServer();
        httpServer.init();
        String[] cols = new String[] { "A", "B", "C" };
        Integer[] types = new Integer[] { 0, 0, 0 };
        httpServer.addServlet(new VisiDBMetadataServlet(cols, types), "/DLRestService/GetQueryMetaDataColumns");
        httpServer.addServlet(new EmptyResponseServlet(), "/DLEmptyResponseRestService/GetQueryMetaDataColumns");
        httpServer.addServlet(new VisiDBMetadataErrorResponseServlet(),
                "/DLErrorResponseRestService/GetQueryMetaDataColumns");
        httpServer.start();
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        super.clearTables();
        httpServer.stop();
    }

    @Test(groups = "functional")
    public void testWriteStringToHdfs() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        modelCommandEntityMgr.createOrUpdate(command);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());
        String hdfsPath = modelStepRetrieveMetadataProcessor.getHdfsPathForMetadataFile(command, commandParameters);
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        }

        try {
            modelStepRetrieveMetadataProcessor.writeStringToHdfs("Contents", hdfsPath);
        } catch (Exception e) {
            Assert.fail("Writing to Hdfs should have passed.");
        }
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, hdfsPath));
        String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, hdfsPath);
        assertTrue(content.equals("Contents"));
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);

        try {
            URL metadataUrl = ClassLoader
                    .getSystemResource("com/latticeengines/dataplatform/service/incorrect-metadata.json");
            String metadata = FileUtils.readFileToString(new File(metadataUrl.getPath()));
            modelStepRetrieveMetadataProcessor.writeStringToHdfs(hdfsPath, metadata);
            Assert.fail("Should have thrown exception.");
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertTrue(((LedpException) e).getCode() == LedpCode.LEDP_16009);
        }
    }

    @Test(groups = "functional")
    public void testSuccessfulExecuteStep() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        modelCommandEntityMgr.createOrUpdate(command);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        modelStepRetrieveMetadataProcessor.executeStepWithResult(command, commandParameters);

        assertTrue(HdfsUtils.fileExists(yarnConfiguration,
                modelStepRetrieveMetadataProcessor.getHdfsPathForMetadataFile(command, commandParameters)));
    }

    @Test(groups = "functional")
    public void testMetadataJsonFormat() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        modelCommandEntityMgr.createOrUpdate(command);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        commandParameters.setDlUrl("http://visidb.lattice-engines.com/DLRestService");
        commandParameters.setDlQuery("View_Table_Cfg_PLS_Event");

        String metadata = modelStepRetrieveMetadataProcessor.executeStepWithResult(command, commandParameters);

        // Assert metadata is in json format
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(metadata);
        long status = (long) jsonObject.get("Status");

        assertEquals(status, 3);
    }

    @Test(groups = "functional")
    public void testMetadataDiagnosticJsonFormatAndExistsInHdfs() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        modelCommandEntityMgr.createOrUpdate(command);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        commandParameters.setDlUrl("http://visidb.lattice-engines.com/DLRestService");
        commandParameters.setDlQuery("View_Table_Cfg_PLS_Event");

        URL metadataUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/service/incorrect-metadata.json");
        String metadata = FileUtils.readFileToString(new File(metadataUrl.getPath()));
        String hdfsPath = modelStepRetrieveMetadataProcessor.getHdfsPathForMetadataDiagnosticsFile(command,
                commandParameters);
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        }

        modelStepRetrieveMetadataProcessor.validateMetadata(metadata, command, commandParameters);
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, hdfsPath));

        // Assert metadataValidationResult is in json format
        String metadataDiagnosticsContent = HdfsUtils.getHdfsFileContents(yarnConfiguration, hdfsPath);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(metadataDiagnosticsContent);
        JSONArray approvedUsageError = (JSONArray) jsonObject.get("ApprovedUsageAnnotationErrors");
        assertEquals(approvedUsageError.size(), 2);
        JSONArray tagsError = (JSONArray) jsonObject.get("TagsAnnotationErrors");
        assertEquals(tagsError.size(), 3);
        JSONArray categoryError = (JSONArray) jsonObject.get("CategoryAnnotationErrors");
        assertEquals(categoryError.size(), 2);
        JSONArray displayNameError = (JSONArray) jsonObject.get("DisplayNameAnnotationErrors");
        assertEquals(displayNameError.size(), 2);
        JSONArray statTypeError = (JSONArray) jsonObject.get("StatisticalTypeAnnotationErrors");
        assertEquals(statTypeError.size(), 2);

        // delete the metadata-diagnostics file
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
    }

    @Test(groups = "functional")
    public void testDoNotUploadMetadataDiagnosticJsonWhenValidationPasses() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        modelCommandEntityMgr.createOrUpdate(command);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        commandParameters.setDlUrl("http://visidb.lattice-engines.com/DLRestService");
        commandParameters.setDlQuery("View_Table_Cfg_PLS_Event");

        String hdfsPath = modelStepRetrieveMetadataProcessor.getHdfsPathForMetadataDiagnosticsFile(command,
                commandParameters);
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        }

        URL metadataUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/service/correct-metadata.json");
        String metadata = FileUtils.readFileToString(new File(metadataUrl.getPath()));

        modelStepRetrieveMetadataProcessor.validateMetadata(metadata, command, commandParameters);

        assertFalse(HdfsUtils.fileExists(yarnConfiguration, hdfsPath));
    }

    @Test(groups = "functional", enabled = true)
    public void testNoResponseExecuteStep() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        commandParameters.setDlUrl("http://localhost:8082/bogusendpoint");

        try {
            modelStepRetrieveMetadataProcessor.executeStep(command, commandParameters);
            Assert.fail("Should have thrown an exception");
        } catch (LedpException e) {
            assertEquals(e.getCode(), LedpCode.LEDP_16005);
        }
    }

    @Test(groups = "functional", enabled = true)
    public void testEmptyResponseExecuteStep() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        commandParameters.setDlUrl("http://localhost:8082/DLEmptyResponseRestService");

        try {
            modelStepRetrieveMetadataProcessor.executeStep(command, commandParameters);
            Assert.fail("Should have thrown an exception");
        } catch (LedpException e) {
            assertEquals(e.getCode(), LedpCode.LEDP_16006);
        }
    }

    @Test(groups = "functional", enabled = true)
    public void testErrorResponseExecuteStep() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        modelCommandEntityMgr.createOrUpdate(command);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        commandParameters.setDlUrl("http://localhost:8082/DLErrorResponseRestService");

        try {
            modelStepRetrieveMetadataProcessor.executeStep(command, commandParameters);
            Assert.fail("Should have thrown an exception");
        } catch (LedpException e) {
            assertEquals(e.getCode(), LedpCode.LEDP_16008);
        }
    }

}
