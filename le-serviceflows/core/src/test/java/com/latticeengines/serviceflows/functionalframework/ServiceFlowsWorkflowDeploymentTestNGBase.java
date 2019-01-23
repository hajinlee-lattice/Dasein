package com.latticeengines.serviceflows.functionalframework;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.util.LinkedMultiValueMap;
import org.testng.annotations.Listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.exposed.service.GlobalAuthTestBed;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceflows-context.xml" })
public abstract class ServiceFlowsWorkflowDeploymentTestNGBase extends WorkflowTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ServiceFlowsWorkflowDeploymentTestNGBase.class);

    protected static final long WORKFLOW_WAIT_TIME_IN_MILLIS = 1000L * 60 * 90;

    @Autowired
    protected GlobalAuthTestBed deploymentTestBed;

    @Value("${common.test.microservice.url}")
    protected String microServiceUrl;

    protected String microServiceHostPort;

    @Value("${common.test.pls.url}")
    protected String plsUrl;

    protected CustomerSpace customer = null;

    protected void setupEnvironment() {
        microServiceHostPort = microServiceUrl.split("//")[1];
        deploymentTestBed.bootstrapForProduct(LatticeProduct.LPA3);
        restTemplate = deploymentTestBed.getRestTemplate();
        magicRestTemplate = deploymentTestBed.getMagicRestTemplate();
        Tenant tenant = deploymentTestBed.getMainTestTenant();
        deploymentTestBed.switchToSuperAdmin();
        customer = CustomerSpace.parse(tenant.getId());
    }

    @SuppressWarnings("rawtypes")
    public SourceFile uploadFile(String resourceBase, String fileName, BusinessEntity businessEntity) {
        log.info("Uploading file " + fileName);

        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(String.format("%s/%s", resourceBase, fileName)));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/pls/models/uploadfile/unnamed?displayName=%s&entity=%s", //
                        plsUrl, fileName, businessEntity.name()), //
                requestEntity, ResponseDocument.class);
        SourceFile sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        log.info(sourceFile.getName());
        return sourceFile;
    }

    @SuppressWarnings("rawtypes")
    public void resolveMetadata(SourceFile sourceFile, SchemaInterpretation schemaInterpretation,
            BusinessEntity businessEntity) {
        log.info("Resolving metadata for modeling ...");
        ModelingParameters parameters = new ModelingParameters();
        parameters.setDescription("Test");
        parameters.setFilename(sourceFile.getName());

        sourceFile.setSchemaInterpretation(schemaInterpretation);
        ResponseDocument response = restTemplate.postForObject(
                String.format("%s/pls/models/uploadfile/%s/fieldmappings?schema=%s&entity=%s", plsUrl,
                        sourceFile.getName(), schemaInterpretation.name(), businessEntity.name()),
                parameters, ResponseDocument.class);
        FieldMappingDocument mappings = new ObjectMapper().convertValue(response.getResult(),
                FieldMappingDocument.class);

        for (FieldMapping mapping : mappings.getFieldMappings()) {
            if (mapping.getMappedField() == null) {
                mapping.setMappedToLatticeField(false);
                mapping.setMappedField(mapping.getUserField().replace(' ', '_'));
            }
        }

        List<String> ignored = new ArrayList<>();
        mappings.setIgnoredFields(ignored);

        log.info("The fieldmappings are: " + mappings.getFieldMappings());
        log.info("The ignored fields are: " + mappings.getIgnoredFields());
        restTemplate.postForObject(
                String.format("%s/pls/models/uploadfile/fieldmappings?displayName=%s", plsUrl, sourceFile.getName()),
                mappings, Void.class);

        Table table = restTemplate.getForObject(
                String.format("%s/pls/fileuploads/%s/metadata", plsUrl, sourceFile.getName()), Table.class);
        sourceFile.setTableName(table.getName());
    }

}
