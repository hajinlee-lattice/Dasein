package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportMatchAndScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.workflow.ImportMatchAndScoreWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class ImportMatchAndScoreWorkflowDeploymentTestNG extends ScoreWorkflowDeploymentTestNG {

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ImportMatchAndScoreWorkflowSubmitter importMatchAndScoreWorkflowSubmitter;

    private SourceFile sourceFile;

    private Table accountTable;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForWorkflow();
        setupTables();
        sourceFile = uploadScoreFile(RESOURCE_BASE + "/csvfiles/Account.csv", accountTable);
        setupModels();
    }

    @Override
    @Test(groups = "deployment", enabled = false)
    public void scoreAccount() throws Exception {
        ModelSummary summary = locateModelSummary("testWorkflowAccount", DEMO_CUSTOMERSPACE);
        assertNotNull(summary);
        score(summary.getId(), sourceFile.getName(), TransformationGroup.STANDARD);
    }

    private void setupTables() throws IOException {
        InputStream ins = getClass().getClassLoader().getResourceAsStream(RESOURCE_BASE + "/tables/Account.json");
        accountTable = JsonUtils.deserialize(IOUtils.toString(ins), Table.class);
        accountTable.setName("ScoreWorkflowDeploymentTest_Account");
        metadataProxy.createTable(MultiTenantContext.getCustomerSpace().toString(), accountTable.getName(),
                accountTable);
    }

    private void score(String modelId, String tableToScore, TransformationGroup transformationGroup) throws Exception {
        ImportMatchAndScoreWorkflowConfiguration configuration = importMatchAndScoreWorkflowSubmitter
                .generateConfiguration(modelId, sourceFile, "Testing Data", transformationGroup);
        WorkflowExecutionId workflowId = workflowService.start(configuration.getWorkflowName(), configuration);

        waitForCompletion(workflowId);
    }

    private SourceFile uploadScoreFile(String resourcePath, Table metadataTable) throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        File file = resolver.getResource(resourcePath).getFile();
        InputStream stream = new FileInputStream(file);
        Tenant tenant = MultiTenantContext.getTenant();
        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        CustomerSpace space = CustomerSpace.parse(tenant.getId());
        String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), space).toString();
        SourceFile sourceFile = new SourceFile();
        sourceFile.setTenant(tenant);
        sourceFile.setName(file.getName());
        sourceFile.setPath(outputPath + "/" + file.getName());
        sourceFile.setSchemaInterpretation(SchemaInterpretation.SalesforceAccount);
        sourceFile.setState(SourceFileState.Uploaded);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, sourceFile.getPath());

        MetadataResolver metadataResolver = new MetadataResolver(sourceFile.getPath(), yarnConfiguration, null) {
        };

        metadataResolver.calculateBasedOnExistingMetadata(metadataTable);
        Table table = metadataResolver.getMetadata();
        System.out.println(table);
        table.setName("SourceFile_" + sourceFile.getName().replace(".", "_"));
        metadataProxy.createTable(tenant.getId(), table.getName(), table);
        sourceFile.setTableName(table.getName());
        internalResourceProxy.updateSourceFile(sourceFile, tenant.getId());
        return internalResourceProxy.findSourceFileByName(sourceFile.getName(), tenant.getId());
    }
}
