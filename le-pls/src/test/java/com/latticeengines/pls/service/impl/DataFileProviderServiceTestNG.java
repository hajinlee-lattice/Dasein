package com.latticeengines.pls.service.impl;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.net.URL;
import java.util.List;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.digest.DigestUtils;
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
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalAuthenticationServiceImpl;
import com.latticeengines.pls.security.GrantedRight;
import com.latticeengines.pls.service.DataFileProviderService;

public class DataFileProviderServiceTestNG extends PlsFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DataFileProviderServiceTestNG.class);

    private Ticket ticket = null;

    @Autowired
    private GlobalAuthenticationServiceImpl globalAuthenticationService;

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

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        assertEquals(ticket.getTenants().size(), 2);
        assertNotNull(ticket);
        String tenant1 = ticket.getTenants().get(0).getId();
        String tenant2 = ticket.getTenants().get(1).getId();
        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant1, "admin");
        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant2, "admin");

        setupDb(tenant1, tenant2);

        String dir = modelingServiceHdfsBaseDir
                + "/BD2_ADEDTBDd69264296nJ26263627n12/models/Q_PLS_Modeling_BD2_ADEDTBDd69264296nJ26263627n12/8e3a9d8c-3bc1-4d21-9c91-0af28afc5c9a/1423547416066_0001/";
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");

        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir + "/enhancements");
        HdfsUtils
                .copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/enhancements/modelsummary.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_model.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_readoutsample.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_scored.txt");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_explorer.csv");

    }

    @Test(groups = { "functional" }, dataProvider = "dataFileProvier")
    public void downloadFile(final String mimeType, final String filter) {

        Tenant tenant;
        List<Tenant> tenants = tenantEntityMgr.findAll();
        if (tenants.get(0).getName().contains("BD_ADEDTBDd70064499nF26263627n1")) {
            tenant = tenants.get(0);
        } else {
            tenant = tenants.get(1);
        }
        setupSecurityContext(tenant);

        List<ModelSummary> summaries = modelSummaryEntityMgr.findAllValid();
        ModelSummary summary;
        if (summaries.get(0).getLookupId().contains("BD2_ADEDTBDd69264296nJ26263627n12")) {
            summary = summaries.get(0);
        } else {
            summary = summaries.get(1);
        }

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream os = mock(ServletOutputStream.class);
        try {
            when(response.getOutputStream()).thenReturn(os);
            dataFileProviderService.downloadFile(request, response, summary.getId(), mimeType, filter);
            verify(response).setHeader(eq("Content-Disposition"), anyString());
            verify(response).setContentType(mimeType);

        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test(groups = { "functional" }, dataProvider = "dataFileProvierNotFound")
    public void downloadFileNotFound(final String mimeType, final String filter) {

        Tenant tenant;
        List<Tenant> tenants = tenantEntityMgr.findAll();
        if (tenants.get(0).getName().contains("BD_ADEDTBDd70064499nF26263627n1")) {
            tenant = tenants.get(0);
        } else {
            tenant = tenants.get(1);
        }
        setupSecurityContext(tenant);

        List<ModelSummary> summaries = modelSummaryEntityMgr.findAllValid();
        ModelSummary summary;
        if (summaries.get(0).getLookupId().contains("BD2_ADEDTBDd69264296nJ26263627n12")) {
            summary = summaries.get(0);
        } else {
            summary = summaries.get(1);
        }

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream os = mock(ServletOutputStream.class);
        try {
            when(response.getOutputStream()).thenReturn(os);
            dataFileProviderService.downloadFile(request, response, summary.getId(), mimeType, filter);

        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof LedpException);
            Assert.assertEquals(((LedpException) ex.getCause()).getCode(), LedpCode.LEDP_18023);
            return;
        }

        Assert.fail("Should not come here.");
    }

    @DataProvider(name = "dataFileProvier")
    public static Object[][] getDataFileProvier() {
        return new Object[][] { { "application/json", "modelsummary.json" }, //
                { "application/csv", ".*_model.csv" }, //
                { "application/csv", ".*_readoutsample.csv" }, //
                { "text/plain", ".*_scored.txt" }, //
                { "application/csv", ".*_explorer.csv" }

        };
    }

    @DataProvider(name = "dataFileProvierNotFound")
    public static Object[][] getDataFileProvierNotFound() {
        return new Object[][] { { "application/json", "modelsummaryNotFound.json" }, //

        };
    }
}
