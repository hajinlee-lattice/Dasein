package com.latticeengines.pls.service.impl;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URL;
import java.util.List;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.DataFileProviderService;
import com.latticeengines.pls.service.TenantService;

public class DataFileProviderServiceTestNG extends PlsFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DataFileProviderServiceTestNG.class);

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

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {

        setupUsers();

        Tenant tenant1 = new Tenant();
        tenant1.setId("TENANT1");
        tenant1.setName("TENANT1");
        tenantService.discardTenant(tenant1);
        tenantService.registerTenant(tenant1);

        setupDbWithEloquaSMB("TENANT1", "TENANT1");

        Tenant tenant = tenantEntityMgr.findByTenantId("TENANT1");
        setupSecurityContext(tenant);

        List<ModelSummary> summaries = modelSummaryEntityMgr.findAllValid();
        ModelSummary summary = summaries.get(0);

        String tokens[] = summary.getLookupId().split("\\|");
        String dir = modelingServiceHdfsBaseDir + "/" + tokens[0] + "/models/" + tokens[1] + "/" + tokens[2] + "/container_01/";
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");

        modelId = summary.getId();
        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir + "/enhancements");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/diagnostics.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/enhancements/modelsummary.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_model.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_readoutsample.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_scored.txt");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_explorer.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/rf_model.txt");

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
        return new Object[][] { { "application/json", "modelsummary.json" }, //
                { "application/json", "diagnostics.json" }, //
                { "application/csv", ".*_model.csv" }, //
                { "application/csv", ".*_readoutsample.csv" }, //
                { "text/plain", ".*_scored.txt" }, //
                { "application/csv", ".*_explorer.csv" }, //
                { "text/plain", "rf_model.txt" }
        };
    }

    @DataProvider(name = "dataFileProviderNotFound")
    public static Object[][] getDataFileProvierNotFound() {
        return new Object[][] { { "application/json", "modelsummaryNotFound.json" }, //

        };
    }
}
