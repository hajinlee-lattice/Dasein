package com.latticeengines.pls.service.impl.dcp;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.auth.exposed.util.TeamUtils;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
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
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.service.DataMappingService;
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

    private static final int DEFAULT_PAGE_SIZE = 20;

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

    @Inject
    private DataMappingService dataMappingService;

    @Inject
    private BatonService batonService;

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
        return getSourceList(projectId, 1, DEFAULT_PAGE_SIZE);
    }

    @Override
    public List<Source> getSourceList(String projectId, int pageIndex, int pageSize) {
        Preconditions.checkArgument(pageIndex > 0);
        Preconditions.checkArgument(pageSize > 0);
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.getSourceList(customerSpace.toString(), projectId, pageIndex - 1, pageSize, TeamUtils.getTeamIds());
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
    public FetchFieldDefinitionsResponse getSourceMappings(String sourceId, String entityType, String fileImportId) {
        Preconditions.checkState(!StringUtils.isAllBlank(sourceId, fileImportId),
                "provide one parameter at least : source Id or import file Id");

        // 1a. Convert systemObject to entity.
        EntityType entityTypeObj = StringUtils.isNotBlank(entityType) ?
                EntityType.valueOf(entityType) : EntityType.Accounts;
        Preconditions.checkState(EntityType.Accounts.equals(entityTypeObj), String.format("illegal entity type %s",
                entityTypeObj));

        // 1b. Generate sourceFile object.
        SourceFile sourceFile = sourceFileService.findByName(fileImportId);

        // 1c. Generate customerSpace.
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        log.info(String.format("Internal Values:\n   entity: %s\n   subType: %s\n" +
                        "   Source File: %s\n   Customer Space: %s", entityTypeObj.getEntity(),
                entityTypeObj.getSubType(),
                sourceFile == null ? "empty" : sourceFile.getName(), customerSpace.toString()));

        // 2a. Set up FetchFieldDefinitionsResponse and Current FieldDefinitionsRecord.
        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse = new FetchFieldDefinitionsResponse();
        fetchFieldDefinitionsResponse.setCurrentFieldDefinitionsRecord(
                new FieldDefinitionsRecord(S3ImportSystem.SystemType.DCP.getDefaultSystemName(),
                        S3ImportSystem.SystemType.DCP.name(),
                        entityTypeObj.getDisplayName()));

        // 2b. Retrieve Spec for given systemType and systemObject.
        String systemType = S3ImportSystem.SystemType.DCP.name();
        if (batonService.isEnabled(customerSpace, LatticeFeatureFlag.MATCH_MAPPING_V2))
            systemType += "-v2";
        fetchFieldDefinitionsResponse.setImportWorkflowSpec(
                importWorkflowSpecProxy.getImportWorkflowSpec(customerSpace.toString(),
                        systemType,
                        entityTypeObj.getDisplayName()));

        // 2c. Find previously saved template matching this customerSpace, sourceId, if it exists.
        setExistingFieldDefinitionsFromSource(customerSpace, fetchFieldDefinitionsResponse, sourceId);

        // 2d. Create a MetadataResolver using the sourceFile.
        if (sourceFile != null) {
            MetadataResolver resolver = getMetadataResolver(sourceFile, null, false);
            fetchFieldDefinitionsResponse.setAutodetectionResultsMap(
                    ImportWorkflowUtils.generateAutodetectionResultsMap(resolver));
        }

        // 3. Generate the initial FieldMappingsRecord based on the Spec, existing table, input file.
        ImportWorkflowUtils.generateCurrentFieldDefinitionRecord(fetchFieldDefinitionsResponse);

        return fetchFieldDefinitionsResponse;
    }

    @Override
    public ValidateFieldDefinitionsResponse validateSourceMappings(
            String fileImportId, String entityType, ValidateFieldDefinitionsRequest validateRequest) throws Exception {
        EntityType entityTypeObj = StringUtils.isNotBlank(entityType) ?
                EntityType.valueOf(entityType) : EntityType.Accounts;
        Preconditions.checkState(EntityType.Accounts.equals(entityTypeObj), String.format("illegal entity type %s",
                entityTypeObj));
        return dataMappingService.validateFieldDefinitions("faked", S3ImportSystem.SystemType.DCP.name(),
                entityType, fileImportId, validateRequest);
    }

    @Override
    public Source updateSource(UpdateSourceRequest updateSourceRequest) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.updateSource(customerSpace.toString(), updateSourceRequest);
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


    private MetadataResolver getMetadataResolver(SourceFile sourceFile, FieldMappingDocument fieldMappingDocument,
                                                 boolean cdlResolve) {
        return new MetadataResolver(sourceFile.getPath(), yarnConfiguration, fieldMappingDocument, cdlResolve, null);
    }

    @Override
    public Boolean reactivateSource(String sourceId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.reactivateSource(customerSpace.toString(), sourceId);
    }
}
