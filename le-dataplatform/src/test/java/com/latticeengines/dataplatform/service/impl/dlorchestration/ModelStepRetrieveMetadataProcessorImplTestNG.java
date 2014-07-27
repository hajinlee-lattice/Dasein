package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.testng.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.functionalframework.StandaloneHttpServer;
import com.latticeengines.dataplatform.functionalframework.VisiDBMetadataServlet;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

@SuppressWarnings("unused")
public class ModelStepRetrieveMetadataProcessorImplTestNG extends DataPlatformFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(ModelStepRetrieveMetadataProcessorImplTestNG.class);

    @Autowired
    private ModelStepRetrieveMetadataProcessorImpl modelStepRetrieveMetadataProcessor;

    @Autowired
    private Configuration yarnConfiguration;

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
        httpServer.start();
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        super.clearTables();
        httpServer.stop();
    }

    @Test(groups = "functional")
    public void testSuccessfulExecuteStep() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());
        commandParameters.setDlUrl("http://localhost:8082/DLRestService");
        modelStepRetrieveMetadataProcessor.executeStep(command, commandParameters);

        assertTrue(HdfsUtils.fileExists(yarnConfiguration,
                modelStepRetrieveMetadataProcessor.getHdfsPathForMetadataFile(command, commandParameters)));
    }

    @Test(groups = "functional", expectedExceptions = LedpException.class, enabled = false)
    public void testFailedExecuteStep() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        commandParameters.setDlUrl("http://httpbin.org/post1");

        modelStepRetrieveMetadataProcessor.executeStep(command, commandParameters);
    }

}
