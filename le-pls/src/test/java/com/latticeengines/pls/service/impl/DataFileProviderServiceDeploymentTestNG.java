package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.List;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;

public class DataFileProviderServiceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DataFileProviderServiceDeploymentTestNG.class);

    @Mock
    private SourceFileProxy sourceFileProxy;

    @Mock
    private SourceFile sourceFile;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    @Spy
    private DataFileProviderServiceImpl dataFileProviderService = new DataFileProviderServiceImpl();

    @Autowired
    private ImportFromS3Service importFromS3Service;

    @Autowired
    private BatonService batonService;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    private String modelId;

    private String fileContents;

    private static String fileFolder;

    private static String tableFileFolder;

    private String TENANT_ID;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        dataFileProviderService.setConfiguration(yarnConfiguration);
        dataFileProviderService.setModelSummaryProxy(modelSummaryProxy);
        dataFileProviderService.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
        dataFileProviderService.setImportFromS3Service(importFromS3Service);
        dataFileProviderService.setBatonService(batonService);

        testBed.bootstrap(1);
        Tenant tenant1 = testBed.getMainTestTenant();
        setupDbWithEloquaSMB(tenant1, true, true);

        TENANT_ID = tenant1.getId();
        Tenant tenant = tenantEntityMgr.findByTenantId(TENANT_ID);
        setupSecurityContext(tenant);

        List<ModelSummary> summaries = modelSummaryProxy.findAllValid(TENANT_ID);
        ModelSummary summary = summaries.get(0);
        modelId = summary.getId();
        String dir = modelingServiceHdfsBaseDir + "/" + TENANT_ID + "/models/ANY_TABLE/" + modelId + "/container_01/";
        fileFolder = dir;
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/modelsummary-eloqua-token.json");
        fileContents = IOUtils.toString(modelSummaryUrl);

        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir + "/enhancements");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/diagnostics.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(),
                dir + "/enhancements/modelsummary.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_model.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_readoutsample.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_scored.txt");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_explorer.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/rf_model.txt");

        dir = modelingServiceHdfsBaseDir + "/" + CustomerSpace.parse(TENANT_ID) + "/data/ANY_TABLE/csv_files";
        tableFileFolder = dir;
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(),
                dir + "/postMatchEventTable_allTraining-r-00000.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(),
                dir + "/postMatchEventTable_allTest-r-00000.csv");

    }

    @AfterClass(groups = { "deployment" })
    public void teardown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/" + TENANT_ID);
    }

    @Test(groups = { "deployment" }, dataProvider = "dataFileProvider", enabled = true)
    public void downloadFile(final String mimeType, final String filter) {

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream os = mock(ServletOutputStream.class);
        try {
            when(response.getOutputStream()).thenReturn(os);
            dataFileProviderService.downloadFile(request, response, modelId, mimeType, filter);
            verify(response, atMost(2)).setHeader(eq("Content-Disposition"), anyString());
            verify(response).setContentType(mimeType);

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = { "deployment" }, dataProvider = "dataFilePathProvider")
    public void testDownloadSourceFileCsv(final String mimeType, final String filePath) {

        when(sourceFileProxy.findByName(any(), anyString())).thenReturn(sourceFile);
        doReturn(filePath).when(sourceFile).getPath();
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream os = mock(ServletOutputStream.class);
        try {
            when(response.getOutputStream()).thenReturn(os);
            dataFileProviderService.downloadSourceFileCsv(request, response, mimeType, filePath, sourceFile);
            verify(response).setHeader(eq("Content-Disposition"), anyString());
            verify(response).setContentType(mimeType);

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = { "deployment" }, dataProvider = "dataFilePathProvider")
    public void downloadFileByPath(final String mimeType, final String filePath) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream os = mock(ServletOutputStream.class);
        try {
            when(response.getOutputStream()).thenReturn(os);
            dataFileProviderService.downloadFileByPath(request, response, mimeType, filePath);
            verify(response).setHeader(eq("Content-Disposition"), anyString());
            verify(response).setContentType(mimeType);

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = { "deployment" }, dataProvider = "dataFilePathProviderNotFound")
    public void downloadFileByPathNotFound(final String mimeType, final String filePath) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream os = mock(ServletOutputStream.class);
        try {
            when(response.getOutputStream()).thenReturn(os);
            dataFileProviderService.downloadFileByPath(request, response, mimeType, filePath);

        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof LedpException);
            return;
        }

        Assert.fail("Should not find file to download.");
    }

    @Test(groups = { "deployment" }, dataProvider = "dataFileProvider", enabled = true)
    public void getFileContents(final String mimeType, final String filter) {
        try {
            String contents = dataFileProviderService.getFileContents(modelId, mimeType, filter);
            assertEquals(contents, fileContents);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = { "deployment" }, dataProvider = "dataFileProviderNotFound", enabled = true)
    public void downloadFileNotFound(final String mimeType, final String filter) {

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream os = mock(ServletOutputStream.class);
        try {
            when(response.getOutputStream()).thenReturn(os);
            dataFileProviderService.downloadFile(request, response, modelId, mimeType, filter);

        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof LedpException);
            Assert.assertEquals(((LedpException) ex.getCause()).getCode(), LedpCode.LEDP_18023);
            return;
        }

        Assert.fail("Should not come here.");
    }

    @DataProvider(name = "dataFileProvider")
    public static Object[][] getDataFileProvier() {
        return new Object[][] { { MediaType.APPLICATION_JSON, "modelsummary.json" }, //
                { MediaType.APPLICATION_JSON, "diagnostics.json" }, //
                { "application/csv", ".*_model.csv" }, //
                { "application/csv", ".*_readoutsample.csv" }, //
                { MediaType.TEXT_PLAIN, ".*_scored.txt" }, //
                { "application/csv", ".*_explorer.csv" }, //
                { "application/csv", "rf_model.txt" }, //
                { MediaType.APPLICATION_OCTET_STREAM, "postMatchEventTable.*Training.*.csv" }, //
                { MediaType.APPLICATION_OCTET_STREAM, "postMatchEventTable.*Test.*.csv" } };
    }

    @DataProvider(name = "dataFileProviderNotFound")
    public static Object[][] getDataFileProvierNotFound() {
        return new Object[][] { { "application/json", "modelsummaryNotFound.json" }, //

        };
    }

    @DataProvider(name = "dataFilePathProvider")
    public static Object[][] getDataFilePathProvier() {
        String csvFilePath = fileFolder + "/test_readoutsample.csv";
        String tainingFilePath = tableFileFolder + "/postMatchEventTable_allTraining-r-00000.csv";
        return new Object[][] { { MediaType.TEXT_PLAIN, csvFilePath }, //
                { MediaType.APPLICATION_OCTET_STREAM, tainingFilePath } };
    }

    @DataProvider(name = "dataFilePathProviderNotFound")
    public static Object[][] getDataFilePathProvierNotFound() {

        return new Object[][] { { "application/json", "modelsummaryNotFound.json" },
                { MediaType.APPLICATION_OCTET_STREAM, "/postMatchEventTable_allTraining-r-00000.csv" }, //

        };
    }
}
