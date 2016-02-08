package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.leadprioritization.workflow.ImportEventTableWorkflowConfiguration;
import com.latticeengines.metadata.exposed.resolution.UserDefinedMetadataResolutionStrategy;
import com.latticeengines.metadata.exposed.resolution.MetadataResolutionStrategy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import com.latticeengines.workflow.exposed.service.SourceFileService;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class ImportEventTableWorkflowTestNGBase extends WorkflowApiFunctionalTestNGBase {

    protected static final CustomerSpace DEMO_CUSTOMERSPACE = CustomerSpace.parse("DemoContract.DemoTenant.Production");

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MetadataProxy metadataProxy;

    protected void setupForImportWorkflow() throws Exception {
        Tenant tenant = setupTenant(DEMO_CUSTOMERSPACE);
        SecurityContextUtils.setTenant(tenant);
        assertNotNull(SecurityContextUtils.getTenant());
        setupUsers(DEMO_CUSTOMERSPACE);
        setupCamille(DEMO_CUSTOMERSPACE);
        setupHdfs(DEMO_CUSTOMERSPACE);
        installServiceFlow("le-serviceflows-leadprioritization", //
                "com.latticeengines.leadprioritization.Initializer");

    }

    protected SourceFile uploadFile(String resourcePath, SchemaInterpretation schema) {
        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            File file = resolver.getResource(resourcePath).getFile();
            InputStream stream = new FileInputStream(file);
            Tenant tenant = SecurityContextUtils.getTenant();
            tenant = tenantEntityMgr.findByTenantId(tenant.getId());
            CustomerSpace space = CustomerSpace.parse(tenant.getId());
            String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), space).toString();
            SourceFile sourceFile = new SourceFile();
            sourceFile.setTenant(tenant);
            sourceFile.setName(file.getName());
            sourceFile.setPath(outputPath + "/" + file.getName());
            sourceFile.setSchemaInterpretation(schema);
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, sourceFile.getPath());

            MetadataResolutionStrategy strategy = new UserDefinedMetadataResolutionStrategy(sourceFile.getPath(),
                    sourceFile.getSchemaInterpretation(), null);
            Table table = strategy.getMetadata();
            table.setName("SourceFile_" + sourceFile.getName().replace(".", "_"));
            metadataProxy.createImportTable(tenant.getId(), table.getName(), table);
            sourceFile.setTableName(table.getName());
            sourceFileService.create(sourceFile);

            return sourceFileService.findByName(sourceFile.getName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected ImportEventTableWorkflowConfiguration generateWorkflowConfig(SourceFile sourceFile) {
        ImportEventTableWorkflowConfiguration workflowConfig = new ImportEventTableWorkflowConfiguration.Builder()
                .customer(DEMO_CUSTOMERSPACE) //
                .microServiceHostPort(microServiceHostPort) //
                .internalResourceHostPort(internalResourceHostPort) //
                .sourceType(SourceType.FILE) //
                .sourceFileName(sourceFile.getName()).build();
        return workflowConfig;
    }

}
