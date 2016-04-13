package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndModelWorkflow;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.pls.metadata.resolution.ColumnTypeMapping;
import com.latticeengines.pls.metadata.resolution.MetadataResolutionStrategy;
import com.latticeengines.pls.metadata.resolution.UserDefinedMetadataResolutionStrategy;
import com.latticeengines.pls.workflow.ImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class ImportMatchAndModelWorkflowDeploymentTestNGBase extends WorkflowApiFunctionalTestNGBase {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ImportMatchAndModelWorkflowDeploymentTestNGBase.class);

    protected static final CustomerSpace DEMO_CUSTOMERSPACE = CustomerSpace.parse("DemoContract.DemoTenant.Production");

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private ImportMatchAndModelWorkflow importMatchAndModelWorkflow;

    @Autowired
    private ImportMatchAndModelWorkflowSubmitter importMatchAndModelWorkflowSubmitter;

    protected void setupForWorkflow() throws Exception {
        Tenant tenant = setupTenant(DEMO_CUSTOMERSPACE);
        MultiTenantContext.setTenant(tenant);
        assertNotNull(MultiTenantContext.getTenant());
        setupUsers(DEMO_CUSTOMERSPACE);
        setupCamille(DEMO_CUSTOMERSPACE);
        setupHdfs(DEMO_CUSTOMERSPACE);
    }

    protected SourceFile uploadFile(String resourcePath, SchemaInterpretation schema) {
        try {
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
            sourceFile.setSchemaInterpretation(schema);
            sourceFile.setState(SourceFileState.Uploaded);
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, sourceFile.getPath());

            MetadataResolutionStrategy strategy = new UserDefinedMetadataResolutionStrategy(sourceFile.getPath(),
                    sourceFile.getSchemaInterpretation(), null, yarnConfiguration);
            strategy.calculate();
            if (!strategy.isMetadataFullyDefined()) {
                List<ColumnTypeMapping> unknown = strategy.getUnknownColumns();
                strategy = new UserDefinedMetadataResolutionStrategy(sourceFile.getPath(),
                        sourceFile.getSchemaInterpretation(), unknown, yarnConfiguration);
                strategy.calculate();
            }
            Table table = strategy.getMetadata();
            table.setName("SourceFile_" + sourceFile.getName().replace(".", "_"));
            metadataProxy.createTable(tenant.getId(), table.getName(), table);
            sourceFile.setTableName(table.getName());
            internalResourceProxy.createSourceFile(sourceFile, tenant.getId());
            return internalResourceProxy.findSourceFileByName(sourceFile.getName(), tenant.getId());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void model(ModelingParameters parameters) throws Exception {
        ImportMatchAndModelWorkflowConfiguration configuration = importMatchAndModelWorkflowSubmitter
                .generateConfiguration(parameters);
        WorkflowExecutionId workflowId = workflowService.start(importMatchAndModelWorkflow.name(), configuration);

        waitForCompletion(workflowId);
    }

}
