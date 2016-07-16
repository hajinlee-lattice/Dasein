package com.latticeengines.pls.service.impl.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;
import com.latticeengines.pls.util.MetadataUtils;

@Component("pythonScriptModelRequiredColumnsExtractor")
public class PythonScriptModelRequiredColumnsExtractor extends GetRequiredColumns {
    
    private static final Log log = LogFactory.getLog(PythonScriptModelRequiredColumnsExtractor.class);

    protected PythonScriptModelRequiredColumnsExtractor() {
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

}
