package com.latticeengines.apps.lp.service.impl;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.lp.service.SourceFileService;
import com.latticeengines.apps.lp.util.MetadataUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CopySourceFileRequest;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("pythonScriptModelService")
public class PythonScriptModelService extends ModelServiceBase {

    private static final Logger log = LoggerFactory.getLogger(PythonScriptModelService.class);

    @Inject
    private SourceFileService sourceFileService;

    protected PythonScriptModelService() {
        super(ModelType.PYTHONMODEL);
    }

    @Override
    public List<Attribute> getRequiredColumns(String modelId) {
        Table eventTable = MetadataUtils.getEventTableFromModelId(modelId, modelSummaryService, metadataProxy);
        if (eventTable.getAttributes() == null) {
            log.error(String.format("Model %s does not have attributes in the event tableName", modelId));
            throw new LedpException(LedpCode.LEDP_18105, new String[] { modelId });
        }
        return getRequiredColumns(eventTable);
    }

    @VisibleForTesting
    List<Attribute> getRequiredColumns(Table eventTable) {
        List<Attribute> attrs = eventTable.getAttributes().stream().filter(
                attr -> attr.isInternalAndInternalTransformField() || attr.getInterfaceName() == InterfaceName.Id)
                .collect(Collectors.toList());
        Set<String> includeParentNames = attrs.stream()
                .filter(attr -> !attr.getParentAttributeNames().isEmpty()
                        && !attr.getApprovedUsage().contains(ApprovedUsage.NONE.toString())) //
                .flatMap(attr -> attr.getParentAttributeNames().stream()) //
                .filter(Objects::nonNull) //
                .distinct() //
                .collect(Collectors.toSet());////
        return attrs.stream()
                .filter(attr -> attr.getInterfaceName() == InterfaceName.Id
                        || includeParentNames.contains(attr.getName())
                        || attr.isInternalPredictor()
                                && !attr.getApprovedUsage().contains(ApprovedUsage.NONE.toString())
                                && !LogicalDataType.isEventTypeOrDerviedFromEventType(attr.getLogicalDataType())
                                && !LogicalDataType.isSystemGeneratedEventType(attr.getLogicalDataType())
                                && !LogicalDataType.isExcludedFromScoringFileMapping(attr.getLogicalDataType()))
                .collect(Collectors.toList());

    }

    @Override
    public Set<String> getLatticeAttributeNames(String modelId) {
        Table eventTable = MetadataUtils.getEventTableFromModelId(modelId, modelSummaryService, metadataProxy);
        if (eventTable.getAttributes() == null) {
            log.error(String.format("Model %s does not have attributes in the event tableName", modelId));
            throw new LedpException(LedpCode.LEDP_18105, new String[] { modelId });
        }
        Set<String> attrNameSet = eventTable.getAttributes().stream() //
                .filter(attr -> attr.getTags() != null && !attr.isInternalPredictor()
                        && !LogicalDataType.isEventTypeOrDerviedFromEventType(attr.getLogicalDataType())
                        && !LogicalDataType.isSystemGeneratedEventType(attr.getLogicalDataType()))
                .map(Attribute::getName).collect(Collectors.toSet());
        log.info("The column names are : " + attrNameSet);
        return attrNameSet;
    }

    @Override
    public String copyModel(ModelSummary modelSummary, String sourceTenantId, String targetTenantId) {
        String newModelGuid;
        String trainingTableName = modelSummary.getTrainingTableName();
        String eventTableName = modelSummary.getEventTableName();

        String cpTrainingTableName = trainingTableName;
        String cpEventTableName = eventTableName;
        if (metadataProxy.getTable(sourceTenantId, trainingTableName) != null) {
            Table cpTrainingTable = metadataProxy.copyTable(sourceTenantId, trainingTableName, targetTenantId);
            cpTrainingTableName = cpTrainingTable.getName();
        }
        if (metadataProxy.getTable(sourceTenantId, eventTableName) != null) {
            Table cpEventTable = metadataProxy.copyTable(sourceTenantId, eventTableName, targetTenantId);
            cpEventTableName = cpEventTable.getName();
        }

        Tenant targetTenant = tenantEntityMgr.findByTenantId(targetTenantId);
        SourceFile sourceFile = sourceFileService.findByTableName(trainingTableName);
        if (sourceFile != null) {
            CopySourceFileRequest request = new CopySourceFileRequest();
            request.setOriginalSourceFile(sourceFile.getName());
            request.setTargetTable(cpTrainingTableName);
            request.setTargetTenant(targetTenant.getId());
            sourceFileService.copySourceFile(request);
        }
        try {
            newModelGuid = copyHdfsData(sourceTenantId, targetTenantId, eventTableName, cpTrainingTableName,
                    cpEventTableName, modelSummary);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_18111,
                    new String[] { modelSummary.getName(), sourceTenantId, targetTenantId });
        }
        return newModelGuid;
    }

}
