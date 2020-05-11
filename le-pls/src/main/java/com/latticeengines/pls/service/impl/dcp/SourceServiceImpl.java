package com.latticeengines.pls.service.impl.dcp;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.UpdateSourceRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.OtherTemplateData;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.service.dcp.SourceService;
import com.latticeengines.pls.util.ImportWorkflowUtils;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.core.ImportWorkflowSpecProxy;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Service("dcpSourceService")
public class SourceServiceImpl implements SourceService {

    private static final Logger log = LoggerFactory.getLogger(SourceServiceImpl.class);

    @Inject
    private SourceProxy sourceProxy;

    @Inject
    private SourceFileService sourceFileService;

    @Inject
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    public Source createSource(SourceRequest sourceRequest) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.createSource(customerSpace.toString(), sourceRequest);
    }

    @Override
    public Source getSource(String sourceId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.getSource(customerSpace.toString(), sourceId);
    }

    @Override
    public List<Source> getSourceList(String projectId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.getSourceList(customerSpace.toString(), projectId);
    }

    @Override
    public Boolean deleteSource(String sourceId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.deleteSource(customerSpace.toString(), sourceId);
    }

    @Override
    public Boolean pauseSource(String sourceId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.pauseSource(customerSpace.toString(), sourceId);
    }

    @Override
    public FetchFieldDefinitionsResponse fetchFieldDefinitions(String sourceId, String entityType,
                                                               String importFile)
            throws Exception {

        // 1a. Convert systemObject to entity.
        EntityType entityTypeObj = StringUtils.isNotBlank(entityType) ?
                EntityType.valueOf(entityType) : EntityType.Accounts;

        // 1b. Generate sourceFile object.
        SourceFile sourceFile = getSourceFile(importFile);

        // 1c. Generate customerSpace.
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        log.info(String.format("Internal Values:\n   entity: %s\n   subType: %s\n" +
                        "   Source File: %s\n   Customer Space: %s", entityTypeObj.getEntity(),
                entityTypeObj.getSubType(),
                sourceFile.getName(), customerSpace.toString()));

        // 2a. Set up FetchFieldDefinitionsResponse and Current FieldDefinitionsRecord.
        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse = new FetchFieldDefinitionsResponse();
        fetchFieldDefinitionsResponse.setCurrentFieldDefinitionsRecord(
                new FieldDefinitionsRecord(S3ImportSystem.SystemType.DCP.getDefaultSystemName(),
                        S3ImportSystem.SystemType.DCP.name(),
                        entityTypeObj.getDisplayName()));

        // 2b. Retrieve Spec for given systemType and systemObject.
        fetchFieldDefinitionsResponse.setImportWorkflowSpec(
                importWorkflowSpecProxy.getImportWorkflowSpec(customerSpace.toString(),
                        S3ImportSystem.SystemType.DCP.name(),
                        entityTypeObj.getDisplayName()));

        // 2c. Find previously saved template matching this customerSpace, sourceId, if it exists.
        setExistingFieldDefinitionsFromSource(customerSpace, fetchFieldDefinitionsResponse, sourceId);

        // 2d. Create a MetadataResolver using the sourceFile.
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, false);
        fetchFieldDefinitionsResponse.setAutodetectionResultsMap(
                ImportWorkflowUtils.generateAutodetectionResultsMap(resolver));


        // 3. Generate the initial FieldMappingsRecord based on the Spec, existing table, input file.
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(fetchFieldDefinitionsResponse);

        return fetchFieldDefinitionsResponse;
    }

    @Override
    public ValidateFieldDefinitionsResponse validateFieldDefinitions(String importFile,
                                                              ValidateFieldDefinitionsRequest validateRequest) {
        // get default spec from s3
        if (validateRequest.getImportWorkflowSpec() == null || MapUtils.isEmpty(validateRequest.getImportWorkflowSpec()
                .getFieldDefinitionsRecordsMap())) {
            throw new RuntimeException("no spec info");
        }

        if (MapUtils.isEmpty(validateRequest.getAutodetectionResultsMap())) {
            throw new RuntimeException("no auto-detected field definition");
        }

        if (validateRequest.getCurrentFieldDefinitionsRecord() == null || MapUtils.isEmpty(
                validateRequest.getCurrentFieldDefinitionsRecord().getFieldDefinitionsRecordsMap())) {
            throw new RuntimeException("no field definition records");
        }

        Map<String, FieldDefinition> autoDetectionResultsMap = validateRequest.getAutodetectionResultsMap();
        Map<String, List<FieldDefinition>> specFieldDefinitionsRecordsMap =
                validateRequest.getImportWorkflowSpec().getFieldDefinitionsRecordsMap();
        Map<String, List<FieldDefinition>> fieldDefinitionsRecordsMap =
                validateRequest.getCurrentFieldDefinitionsRecord().getFieldDefinitionsRecordsMap();
        Map<String, FieldDefinition> existingFieldDefinitionMap = validateRequest.getExistingFieldDefinitionsMap();
        Map<String, OtherTemplateData> otherTemplateDataMap = validateRequest.getOtherTemplateDataMap();

        // 1 Generate source file and resolver
        SourceFile sourceFile = getSourceFile(importFile);
        MetadataResolver resolver = getMetadataResolver(sourceFile, null, false);

        // 2. generate validation message
        ValidateFieldDefinitionsResponse response =
                ImportWorkflowUtils.generateValidationResponse(fieldDefinitionsRecordsMap, autoDetectionResultsMap,
                        specFieldDefinitionsRecordsMap, existingFieldDefinitionMap, otherTemplateDataMap, resolver);
        // set field definition records map for ui
        response.setFieldDefinitionsRecordsMap(fieldDefinitionsRecordsMap);
        return response;
    }

    @Override
    public Source updateSource(String sourceId, UpdateSourceRequest updateSourceRequest) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.updateSource(customerSpace.toString(), sourceId, updateSourceRequest);
    }

    @Override
    public FieldDefinitionsRecord getSourceMappings(String sourceId) {
        // 1a. default entity type.
        EntityType entityTypeObj = EntityType.Accounts;

        // 1b. Generate customerSpace.
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        log.info(String.format("Internal Values:\n   entity: %s\n   subType: %s\n" +
                        "   Customer Space: %s", entityTypeObj.getEntity(),
                entityTypeObj.getSubType(), customerSpace.toString()));

        // 2a. Set up FetchFieldDefinitionsResponse and Current FieldDefinitionsRecord.
        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse = new FetchFieldDefinitionsResponse();
        fetchFieldDefinitionsResponse.setCurrentFieldDefinitionsRecord(
                new FieldDefinitionsRecord(S3ImportSystem.SystemType.DCP.getDefaultSystemName(),
                        S3ImportSystem.SystemType.DCP.name(),
                        entityTypeObj.getDisplayName()));

        // 2b. Retrieve Spec for given systemType and systemObject.
        fetchFieldDefinitionsResponse.setImportWorkflowSpec(
                importWorkflowSpecProxy.getImportWorkflowSpec(customerSpace.toString(),
                        S3ImportSystem.SystemType.DCP.name(),
                        entityTypeObj.getDisplayName()));

        // 2c. Find previously saved template matching this customerSpace, sourceId, if it exists.
        setExistingFieldDefinitionsFromSource(customerSpace, fetchFieldDefinitionsResponse, sourceId);

        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(fetchFieldDefinitionsResponse);
        return fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
    }

    private void setExistingFieldDefinitionsFromSource(CustomerSpace customerSpace,
                                                       FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse,
                                                       String sourceId) {
        if (StringUtils.isNotBlank(sourceId)) {
            DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTaskBySourceId(customerSpace.toString(), sourceId);
            Preconditions.checkNotNull(dataFeedTask, String.format("Can't retrieve data feed task from source %s",
                    sourceId));
            Table existingTable = dataFeedTask.getImportTemplate();
            Preconditions.checkNotNull(existingTable, String.format("Can't retrieve template table from source %s",
                    sourceId));
            fetchFieldDefinitionsResponse.setExistingFieldDefinitionsMap(
                    ImportWorkflowUtils.getFieldDefinitionsMapFromTable(existingTable));
        }
    }

    private SourceFile getSourceFile(String sourceFileName) {
        SourceFile sourceFile = sourceFileService.findByName(sourceFileName);
        if (sourceFile == null) {
            throw new RuntimeException(String.format("Could not locate source file with name %s", sourceFileName));
        }
        return sourceFile;
    }


    private MetadataResolver getMetadataResolver(SourceFile sourceFile, FieldMappingDocument fieldMappingDocument,
                                                 boolean cdlResolve) {
        return new MetadataResolver(sourceFile.getPath(), yarnConfiguration, fieldMappingDocument, cdlResolve, null);
    }

}
