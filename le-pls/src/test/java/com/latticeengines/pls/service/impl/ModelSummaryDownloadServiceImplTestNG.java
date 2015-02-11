package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.AsyncTaskExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.KeyValueEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class ModelSummaryDownloadServiceImplTestNG extends PlsFunctionalTestNGBase {
    
    @Autowired
    private KeyValueEntityMgr keyValueEntityMgr;

    @Autowired
    private ModelSummaryDownloadServiceImpl modelSummaryDownloadService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired
    private AsyncTaskExecutor modelSummaryDownloadExecutor;
    
    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        modelSummaryDownloadService.setTenantEntityMgr(tenantEntityMgr);
        modelSummaryDownloadService.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
        modelSummaryDownloadService.setYarnConfiguration(yarnConfiguration);
        modelSummaryDownloadService.setModelSummaryEntityMgr(modelSummaryEntityMgr);
        modelSummaryDownloadService.setModelSummaryDownloadExecutor(modelSummaryDownloadExecutor);
        modelSummaryDownloadService.setModelSummaryParser(modelSummaryParser);
        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/TENANT1");
        tenantEntityMgr.deleteAll();
        keyValueEntityMgr.deleteAll();
    }

    @Test(groups = "functional")
    public void executeInternal() throws Exception {
        Tenant tenant = new Tenant();
        tenant.setId("TENANT1");
        tenant.setName("TENANT1");
        tenantEntityMgr.create(tenant);
        
        String dir = modelingServiceHdfsBaseDir
                + "/TENANT1/models/Q_EventTable_TENANT1/58e6de15-5448-4009-a512-bd27d59ca75d/1423547416066_0001/enhancements";
        URL modelSummaryUrl = ClassLoader.getSystemResource(
                "com/latticeengines/pls/functionalframework/modelsummary.json");

        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/modelsummary.json");
        
        modelSummaryDownloadService.executeInternal(null);
        
        setupSecurityContext(tenant);
        List<ModelSummary> summaries = modelSummaryEntityMgr.findAll();
        assertEquals(summaries.size(), 1);
        
    }
}
