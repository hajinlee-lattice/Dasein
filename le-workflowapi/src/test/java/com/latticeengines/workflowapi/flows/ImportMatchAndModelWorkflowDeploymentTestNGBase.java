package com.latticeengines.workflowapi.flows;

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
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryParser;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiDeploymentTestNGBase;

public abstract class ImportMatchAndModelWorkflowDeploymentTestNGBase extends WorkflowApiDeploymentTestNGBase {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ImportMatchAndModelWorkflowDeploymentTestNGBase.class);

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataProxy metadataProxy;

    private ModelSummaryParser modelSummaryParser = new ModelSummaryParser();

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

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
            plsInternalProxy.createSourceFile(sourceFile, tenant.getId());
            return plsInternalProxy.findSourceFileByName(sourceFile.getName(), tenant.getId());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void createModelSummary(ModelSummary model, String tenantId) {
        modelSummaryProxy.createModelSummary(tenantId, model, false);
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
