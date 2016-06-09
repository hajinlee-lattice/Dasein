package com.latticeengines.pls.service.impl;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.DataFileProviderService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;

public class DataFileProviderServiceTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DataFileProviderServiceTestNG.class);
    private static final String TENANT_ID = "TENANT1";

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataFileProviderService dataFileProviderService;

    @Autowired
    private TenantService tenantService;

    private String modelId;

    private String fileContents;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {

        setupUsers();

        Tenant tenant1 = new Tenant();
        tenant1.setId(TENANT_ID);
        tenant1.setName(TENANT_ID);
        tenantService.discardTenant(tenant1);
        tenantService.registerTenant(tenant1);

        setupDbWithEloquaSMB(TENANT_ID, TENANT_ID);

        Tenant tenant = tenantEntityMgr.findByTenantId(TENANT_ID);
        setupSecurityContext(tenant);

        List<ModelSummary> summaries = modelSummaryEntityMgr.findAllValid();
        ModelSummary summary = summaries.get(0);
        modelId = summary.getId();
        String dir = modelingServiceHdfsBaseDir + "/" + TENANT_ID + "/models/ANY_TABLE/" + modelId + "/container_01/";
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");
        fileContents = IOUtils.toString(modelSummaryUrl);

        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir + "/enhancements");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/diagnostics.json");
        HdfsUtils
                .copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/enhancements/modelsummary.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_model.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_readoutsample.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_scored.txt");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_explorer.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/rf_model.txt");

        dir = modelingServiceHdfsBaseDir + "/" + CustomerSpace.parse(TENANT_ID) + "/data/ANY_TABLE/csv_files";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir
                + "/postMatchEventTable_allTraining-r-00000.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir
                + "/postMatchEventTable_allTest-r-00000.csv");

    }

    @AfterClass(groups = { "functional" })
    public void teardown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/" + TENANT_ID);
    }

    @Test(groups = { "functional" }, dataProvider = "dataFileProvider", enabled = true)
    public void downloadFile(final String mimeType, final String filter) {

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream os = mock(ServletOutputStream.class);
        try {
            when(response.getOutputStream()).thenReturn(os);
            dataFileProviderService.downloadFile(request, response, modelId, mimeType, filter);
            verify(response).setHeader(eq("Content-Disposition"), anyString());
            verify(response).setContentType(mimeType);

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = { "functional" }, dataProvider = "dataFileProvider", enabled = true)
    public void getFileContents(final String mimeType, final String filter) {
        try {
            String contents = dataFileProviderService.getFileContents(modelId, mimeType, filter);
            assertEquals(contents, fileContents);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = { "functional" }, dataProvider = "dataFileProviderNotFound", enabled = true)
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
                { MediaType.TEXT_PLAIN, "rf_model.txt" }, //
                { MediaType.APPLICATION_OCTET_STREAM, "postMatchEventTable.*Training.*.csv" }, //
                { MediaType.APPLICATION_OCTET_STREAM, "postMatchEventTable.*Test.*.csv" } };
    }

    @DataProvider(name = "dataFileProviderNotFound")
    public static Object[][] getDataFileProvierNotFound() {
        return new Object[][] { { "application/json", "modelsummaryNotFound.json" }, //

        };
    }
}
