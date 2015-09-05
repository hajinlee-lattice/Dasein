package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.AsyncTaskExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.mbean.TimeStampContainer;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;

public class ModelSummaryDownloadServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private ModelSummaryDownloadServiceImpl modelSummaryDownloadService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired
    private AsyncTaskExecutor modelSummaryDownloadExecutor;
    
    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Autowired
    private TimeStampContainer timeStampContainer;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    private static final String TENANT_ID = "MS_DOWNLOAD_TEST";
    private static final String UUID="8195dcf3-0898-4ad3-b94d-0d0f806e979e";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        modelSummaryDownloadService.setTenantEntityMgr(tenantEntityMgr);
        modelSummaryDownloadService.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
        modelSummaryDownloadService.setYarnConfiguration(yarnConfiguration);
        modelSummaryDownloadService.setModelSummaryEntityMgr(modelSummaryEntityMgr);
        modelSummaryDownloadService.setModelSummaryDownloadExecutor(modelSummaryDownloadExecutor);
        modelSummaryDownloadService.setModelSummaryParser(modelSummaryParser);
        modelSummaryDownloadService.setTimeStampContainer(timeStampContainer);
        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/" + CustomerSpace.parse(TENANT_ID));
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        Tenant tenant = tenantService.findByTenantId(newTenant().getId());
        if (tenant != null) {
            setupSecurityContext(tenant);
            List<ModelSummary> summaries = modelSummaryEntityMgr.findAll();
            for (ModelSummary summary: summaries) {
                modelSummaryEntityMgr.delete(summary);
            }
        }
        tenantService.discardTenant(newTenant());
    }
    
    @BeforeMethod(groups = "functional")
    public void setupMethod() throws Exception { teardown(); }

    @Test(groups = "functional")
    public void executeInternalWithTenantRegistrationEarlierThanHdfsModelCreation() throws Exception {
        Tenant tenant = newTenant();
        tenantService.registerTenant(tenant);
        uploadModelSummary();
        modelSummaryDownloadService.executeInternal(null);

        Thread.sleep(1000L);

        setupSecurityContext(tenant);
        List<ModelSummary> summaries = modelSummaryEntityMgr.findAll();
        assertEquals(summaries.size(), 1, "One new summaries should have been created");
        
        modelSummaryDownloadService.executeInternal(null);
        summaries = modelSummaryEntityMgr.findAll();
        assertEquals(summaries.size(), 1, "No new summaries should have been created");
    }

    @Test(groups = "functional")
    public void executeInternalWithTenantRegistrationLaterThanHdfsModelCreation() throws Exception {
        uploadModelSummary();
        Thread.sleep(5000L);
        tenantService.registerTenant(newTenant());
        
        modelSummaryDownloadService.executeInternal(null);
        setupSecurityContext(newTenant());
        List<ModelSummary> summaries = modelSummaryEntityMgr.findAll();
        assertEquals(summaries.size(), 0, "No new summaries should have been created");
    }

    @Test(groups = "functional")
    public void downloadDetailsOnlyModelSummary() throws Exception {
        tenantService.registerTenant(newTenant());
        uploadDetailsOnlyModelSummary();
        modelSummaryDownloadService.executeInternal(null);

        setupSecurityContext(newTenant());
        List<ModelSummary> summaries = modelSummaryEntityMgr.findAll();
        assertEquals(summaries.size(), 1, "One new summaries should have been created");

        modelSummaryDownloadService.executeInternal(null);
        summaries = modelSummaryEntityMgr.findAll();
        assertEquals(summaries.size(), 1, "No new summaries should have been created");
    }

    private void uploadModelSummary() throws Exception {
        String dir = modelingServiceHdfsBaseDir
                + "/" + CustomerSpace.parse(TENANT_ID) + "/models/Q_EventTable_TENANT1/" + UUID + "/1423547416066_0001/enhancements";
        URL modelSummaryUrl = ClassLoader.getSystemResource(
                "com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");
        HdfsUtils.rmdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/modelsummary.json");
    }

    private void uploadDetailsOnlyModelSummary() throws Exception {
        String dir = modelingServiceHdfsBaseDir
                + "/" + CustomerSpace.parse(TENANT_ID) + "/models/Q_EventTable_TENANT1/" + UUID + "/1423547416066_0001/enhancements";
        URL modelSummaryUrl = ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/modelsummary-detailsonly.json");
        HdfsUtils.rmdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/modelsummary.json");
    }

    private Tenant newTenant() {
        Tenant tenant = new Tenant();
        tenant.setId(CustomerSpace.parse(TENANT_ID).toString());
        tenant.setName(TENANT_ID);
        return tenant;
    }
}
