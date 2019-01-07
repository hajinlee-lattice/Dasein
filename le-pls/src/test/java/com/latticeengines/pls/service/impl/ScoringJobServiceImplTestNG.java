package com.latticeengines.pls.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.ScoringJobService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.service.TenantService;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

public class ScoringJobServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    private static final String TENANT1 = "TestTenant_QuoteProtectionScoringResult";
    private static final String TESTFILENAME = "scoreResult.csv";
    private Tenant tenant;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private ScoringJobService scoringJobService;

    @Inject
    private Configuration yarnConfiguration;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        tenant = tenantService.findByTenantId(TENANT1);
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }

        tenant = new Tenant();
        tenant.setId(TENANT1);
        tenant.setName(TENANT1);
        tenantService.registerTenant(tenant);

        setupSecurityContext(tenant);
    }

    @AfterClass(groups = { "functional" })
    public void teardown() throws Exception {
        tenantService.discardTenant(tenant);
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), TENANT1).toString());
    }

    @Test(groups = "functional")
    public void testGetScoreResults() throws Exception {
        String hdfsDir = PathBuilder.buildDataFileExportPath(CamilleEnvironment.getPodId(), CustomerSpace.parse(TENANT1)).toString();
        String filePrefix = "scoreResults";
        String hdfsPath = hdfsDir + "/" + filePrefix;

        HdfsUtils.mkdir(yarnConfiguration, hdfsDir);
        String localPath = ClassLoader.getSystemResource("com/latticeengines/pls/service/impl/" + TESTFILENAME).getPath();
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, localPath, hdfsPath);

        Job job = new Job();
        Map<String, String> outputs = new HashMap<>();
        outputs.put(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH, hdfsPath);
        job.setOutputs(outputs);

        WorkflowProxy workflowProxy = Mockito.mock(WorkflowProxy.class);
        when(workflowProxy.getWorkflowExecution(anyString(), anyString())).thenReturn(job);
        ReflectionTestUtils.setField(scoringJobService, "workflowProxy", workflowProxy);

        List<String> paths = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, filePrefix + ".*");
        assertEquals(paths.size(), 1);

        scoringJobService.getScoreResults("workflowJobId");
        paths = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, "qp_" + filePrefix + ".*");
        assertEquals(paths.size(), 1);
    }
}
