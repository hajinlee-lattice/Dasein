package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.pls.workflow.ImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiDeploymentTestNGBase;

public class ImportMatchAndModelWorkflowDeploymentTestNGBase extends WorkflowApiDeploymentTestNGBase {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ImportMatchAndModelWorkflowDeploymentTestNGBase.class);

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Autowired
    private ImportMatchAndModelWorkflowSubmitter importMatchAndModelWorkflowSubmitter;

    SourceFile uploadFile(String resourcePath, SchemaInterpretation schema) {
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
            sourceFile.setDisplayName(file.getName());
            sourceFile.setPath(outputPath + "/" + file.getName());
            sourceFile.setSchemaInterpretation(schema);
            sourceFile.setState(SourceFileState.Uploaded);
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, sourceFile.getPath());

            MetadataResolver metadataResolver = new MetadataResolver(sourceFile.getPath(), yarnConfiguration, null);
            FieldMappingDocument fieldMappingDocument = metadataResolver.getFieldMappingsDocumentBestEffort(
                    SchemaRepository.instance().getSchema(sourceFile.getSchemaInterpretation()));
            mapFieldToCustomeFieldsWithSameName(fieldMappingDocument);
            metadataResolver.setFieldMappingDocument(fieldMappingDocument);
            metadataResolver.calculateBasedOnFieldMappingDocument(
                    SchemaRepository.instance().getSchema(sourceFile.getSchemaInterpretation()));

            Table table = metadataResolver.getMetadata();
            System.out.println(table);
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
        WorkflowExecutionId workflowId = workflowService.start(configuration);

        waitForCompletion(workflowId);
    }

    String getModelSummary(String name) {
        List<ModelSummary> summaries = modelSummaryEntityMgr.findAllValid();
        String lookupId = null;
        for (ModelSummary summary : summaries) {
            if (summary.getName().startsWith(name)) {
                lookupId = summary.getLookupId();
                assertEquals(summary.getStatus(), ModelSummaryStatus.INACTIVE);
            }
        }

        String[] tokens = lookupId.split("\\|");
        String path = "/user/s-analytics/customers/%s/models/%s/%s";
        path = String.format(path, tokens[0], tokens[1], tokens[2]);

        assertNotNull(lookupId,
                String.format("Could not find active model summary created with provided name %s", name));

        try {
            List<String> modelPaths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, path,
                    file -> file.getPath().getName().contains("_model.json"));
            assertEquals(modelPaths.size(), 1);
            String modelPath = modelPaths.get(0);
            return HdfsUtils.getHdfsFileContents(yarnConfiguration, modelPath);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    ModelSummary locateModelSummary(String name, CustomerSpace space) {
        String startingHdfsPoint = "/user/s-analytics/customers/" + space;
        HdfsUtils.HdfsFileFilter filter = file -> {
            if (file == null) {
                return false;
            }
            String name1 = file.getPath().getName();
            return name1.equals("modelsummary.json");
        };

        try {
            List<String> files = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, startingHdfsPoint, filter);
            for (String file : files) {
                String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, file);
                ModelSummary summary = modelSummaryParser.parse(file, contents);
                if (name.equals(modelSummaryParser.parseOriginalName(summary.getName()))) {
                    return summary;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    private void mapFieldToCustomeFieldsWithSameName(FieldMappingDocument fieldMappingDocument) {
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setFieldType(UserDefinedType.TEXT);
            }
        }
    }

}
