package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.MetadataUtils;

@Component("pythonScriptModelService")
public class PythonScriptModelService extends ModelServiceBase {

    private static final Logger log = LoggerFactory.getLogger(PythonScriptModelService.class);

    @Autowired
    private SourceFileService sourceFileService;

    protected PythonScriptModelService() {
        super(ModelType.PYTHONMODEL);
    }

    @Override
    public List<Attribute> getRequiredColumns(String modelId) {
        Table eventTable = MetadataUtils.getEventTableFromModelId(modelId, modelSummaryEntityMgr, metadataProxy);
        if (eventTable.getAttributes() == null) {
            log.error(String.format("Model %s does not have attributes in the event tableName", modelId));
            throw new LedpException(LedpCode.LEDP_18105, new String[] { modelId });
        }
        return getRequiredColumns(eventTable);
    }

    @VisibleForTesting
    List<Attribute> getRequiredColumns(Table eventTable) {
        List<Attribute> attrs = eventTable.getAttributes().stream()
                .filter(attr -> attr.isCustomerPredictor() || attr.getInterfaceName() == InterfaceName.Id)
                .collect(Collectors.toList());
        Set<String> excludeParentNames = attrs.stream()
                .filter(attr -> attr.getApprovedUsage() != null
                        && attr.getApprovedUsage().contains(ApprovedUsage.NONE.toString()))
                .flatMap(attr -> attr.getParentAttributeNames().stream())//
                .filter(Objects::nonNull) //
                .distinct() //
                .filter(name -> eventTable.getAttribute(name) != null
                        && eventTable.getAttribute(name).getApprovedUsage() != null
                        && eventTable.getAttribute(name).getApprovedUsage().contains(ApprovedUsage.NONE.toString()))//
                .collect(Collectors.toSet());
        return attrs.stream()
                .filter(attr -> attr.isInternalPredictor() && !excludeParentNames.contains(attr.getName())
                        && !LogicalDataType.isEventTypeOrDerviedFromEventType(attr.getLogicalDataType())
                        && !LogicalDataType.isSystemGeneratedEventType(attr.getLogicalDataType())
                        && !LogicalDataType.isExcludedFromScoringFileMapping(attr.getLogicalDataType()))
                .filter(Objects::nonNull) //
                .collect(Collectors.toList());

    }

    @Override
    public Set<String> getLatticeAttributeNames(String modelId) {
        Table eventTable = MetadataUtils.getEventTableFromModelId(modelId, modelSummaryEntityMgr, metadataProxy);
        if (eventTable.getAttributes() == null) {
            log.error(String.format("Model %s does not have attributes in the event tableName", modelId));
            throw new LedpException(LedpCode.LEDP_18105, new String[] { modelId });
        }
        Set<String> attrNameSet = eventTable.getAttributes().stream() //
                .filter(attr -> attr.getTags() != null && !attr.isInternalPredictor()
                        && !LogicalDataType.isEventTypeOrDerviedFromEventType(attr.getLogicalDataType())
                        && !LogicalDataType.isSystemGeneratedEventType(attr.getLogicalDataType()))
                .filter(Objects::nonNull)//
                .map(Attribute::getName).collect(Collectors.toSet());
        log.info("The column names are : " + attrNameSet);
        return attrNameSet;
    }

    @Override
    public boolean copyModel(ModelSummary modelSummary, String sourceTenantId, String targetTenantId) {
        String trainingTableName = modelSummary.getTrainingTableName();
        String eventTableName = modelSummary.getEventTableName();

        Table cpTrainingTable = metadataProxy.copyTable(sourceTenantId, trainingTableName, targetTenantId);
        Table cpEventTable = metadataProxy.copyTable(sourceTenantId, eventTableName, targetTenantId);

        Tenant targetTenant = tenantEntityMgr.findByTenantId(targetTenantId);
        SourceFile sourceFile = sourceFileService.findByTableName(trainingTableName);
        if (sourceFile != null) {
            sourceFileService.copySourceFile(cpTrainingTable.getName(), sourceFile, targetTenant);
        }
        try {
            copyHdfsData(sourceTenantId, targetTenantId, eventTableName, cpTrainingTable.getName(),
                    cpEventTable.getName(), modelSummary);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_18111,
                    new String[] { modelSummary.getName(), sourceTenantId, targetTenantId });
        }
        return true;
    }

}
