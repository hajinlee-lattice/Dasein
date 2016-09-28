package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;
import com.latticeengines.pls.util.MetadataUtils;

@Component("pythonScriptModelService")
public class PythonScriptModelService extends ModelServiceBase {

    private static final Log log = LogFactory.getLog(PythonScriptModelService.class);

    @Autowired
    private SourceFileService sourceFileService;

    protected PythonScriptModelService() {
        super(ModelType.PYTHONMODEL);
    }

    @Override
    public List<Attribute> getRequiredColumns(String modelId) {
        List<Attribute> requiredColumns = new ArrayList<>();
        Table eventTable = MetadataUtils.getEventTableFromModelId(modelId, modelSummaryEntityMgr, metadataProxy);
        List<Attribute> attributes = eventTable.getAttributes();
        if (attributes == null) {
            log.error(String.format("Model %s does not have attributes in the event tableName", modelId));
            throw new LedpException(LedpCode.LEDP_18105, new String[] { modelId });
        }
        ModelSummary summary = modelSummaryEntityMgr.getByModelId(modelId);
        Table schema = SchemaRepository.instance().getSchema(
                SchemaInterpretation.valueOf(summary.getSourceSchemaInterpretation()));
        for (Attribute attribute : attributes) {
            List<String> tags = attribute.getTags();
            if (schema.getAttribute(attribute.getName()) != null //
                    || (tags != null && !tags.isEmpty() && tags.get(0).equals(Tag.INTERNAL.toString()) //
                    && !(attribute.getApprovedUsage() == null || attribute.getApprovedUsage().isEmpty() || attribute
                            .getApprovedUsage().get(0).equals(ApprovedUsage.NONE.toString())))) {
                LogicalDataType logicalDataType = attribute.getLogicalDataType();
                if (!LogicalDataType.isEventTypeOrDerviedFromEventType(logicalDataType)
                        && !LogicalDataType.isSystemGeneratedEventType(logicalDataType)) {
                    requiredColumns.add(attribute);
                }
            }
        }
        log.info("The required columns are : " + Arrays.toString(requiredColumns.toArray()));
        return requiredColumns;
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
            sourceFileService.copySourceFile(cpTrainingTable, sourceFile, targetTenant);
        }
        try {
            copyHdfsData(sourceTenantId, targetTenantId, eventTableName, cpTrainingTable.getName(),
                    cpEventTable.getName(), modelSummary);
        } catch (IOException e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            throw new LedpException(LedpCode.LEDP_18111, new String[] { modelSummary.getName(), sourceTenantId,
                    targetTenantId });
        }
        return true;
    }

}
