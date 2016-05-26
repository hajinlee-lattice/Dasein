package com.latticeengines.pls.service.impl.fileprocessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.VdbMetadataConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("modelMetadataService")
public class ModelMetadataServiceImpl implements ModelMetadataService {

    private static final Log log = LogFactory.getLog(ModelMetadataServiceImpl.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Override
    public List<VdbMetadataField> getMetadata(String modelId) {
        Table table = getEventTableFromModelId(modelId);
        List<VdbMetadataField> fields = new ArrayList<>();
        for (Attribute attribute : table.getAttributes()) {
            VdbMetadataField field = getFieldFromAttribute(attribute);
            fields.add(field);
        }
        return fields;
    }

    @Override
    public Table cloneTrainingTable(String modelId) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        Table trainingTable = getTrainingTableFromModelId(modelId);
        Table clone = metadataProxy.cloneTable(customerSpace, trainingTable.getName());
        return clone;
    }

    @Override
    public List<String> getRequiredColumnDisplayNames(String modelId) {
        List<String> requiredColumnDisplayNames = new ArrayList<String>();
        List<Attribute> requiredColumns = getRequiredColumns(modelId);
        for (Attribute column : requiredColumns) {
            requiredColumnDisplayNames.add(column.getDisplayName());
        }
        return requiredColumnDisplayNames;
    }

    @Override
    public List<Attribute> getRequiredColumns(String modelId) {
        List<Attribute> requiredColumns = new ArrayList<>();
        Table trainingTable = getTrainingTableFromModelId(modelId);
        List<Attribute> attributes = trainingTable.getAttributes();
        if (attributes == null) {
            log.error(String.format("Model %s does not have attribuets in the event tableName", modelId));
            throw new LedpException(LedpCode.LEDP_18105, new String[] { modelId });
        }
        for (Attribute attribute : attributes) {
            LogicalDataType logicalDataType = attribute.getLogicalDataType();
            if (!LogicalDataType.isEventTypeOrDerviedFromEventType(logicalDataType)
                    && !LogicalDataType.isSystemGeneratedEventType(logicalDataType)) {
                requiredColumns.add(attribute);
            }
        }
        log.info("The required columns are : " + Arrays.toString(requiredColumns.toArray()));
        return requiredColumns;
    }

    @Override
    public List<Attribute> getAttributesFromFields(List<Attribute> attributes, List<VdbMetadataField> fields) {
        List<Attribute> attributeCopy = new ArrayList<>(attributes);
        List<Attribute> editedAttributes = new ArrayList<>();

        for (VdbMetadataField field : fields) {
            boolean found = false;
            for (Attribute attribute : attributes) {
                if (attribute.getName().equals(field.getColumnName())) {
                    attributeCopy.remove(attribute);
                    editedAttributes.add(overwriteAttributeWithFieldValues(attribute, field));
                    found = true;
                    break;
                }
            }
            if (!found) {
                Attribute newAttribute = new Attribute();
                newAttribute.setName(field.getColumnName());
                overwriteAttributeWithFieldValues(newAttribute, field);
                attributeCopy.add(newAttribute);
            }
        }

        attributeCopy.addAll(editedAttributes);
        return attributeCopy;
    }

    private Attribute overwriteAttributeWithFieldValues(Attribute attribute, VdbMetadataField field) {
        if (field.getDisplayName() != null) {
            attribute.setDisplayName(field.getDisplayName());
        }
        if (field.getDescription() != null) {
            attribute.setDescription(field.getDescription());
        }
        if (field.getApprovedUsage() != null) {
            attribute.setApprovedUsage(field.getApprovedUsage());
        }
        if (field.getDisplayDiscretization() != null) {
            attribute.setDisplayDiscretizationStrategy(field.getDisplayDiscretization());
        }
        if (field.getFundamentalType() != null) {
            attribute.setFundamentalType(field.getFundamentalType());
        }
        if (field.getStatisticalType() != null) {
            attribute.setStatisticalType(field.getStatisticalType());
        }
        if (field.getCategory() != null) {
            attribute.setCategory(field.getCategory());
        }
        return attribute;
    }

    private VdbMetadataField getFieldFromAttribute(Attribute attribute) {
        VdbMetadataField field = new VdbMetadataField();

        field.setColumnName(attribute.getName());
        if (attribute.getApprovedUsage() != null && attribute.getApprovedUsage().size() != 0) {
            field.setApprovedUsage(attribute.getApprovedUsage().get(0));
        }
        if (attribute.getDataSource() != null && attribute.getDataSource().size() != 0) {
            field.setSource(attribute.getDataSource().get(0));
        }
        field.setCategory(attribute.getCategory());
        field.setDisplayName(attribute.getDisplayName());
        field.setDescription(attribute.getDescription());
        if (attribute.getTags() != null && attribute.getTags().size() != 0) {
            field.setTags(attribute.getTags().get(0));
        }
        field.setFundamentalType(attribute.getFundamentalType());
        field.setDisplayDiscretization(attribute.getDisplayDiscretizationStrategy());
        field.setStatisticalType(attribute.getStatisticalType());
        field.setSourceToDisplay(getSourceToDisplay(field.getSource()));

        return field;
    }

    private String getSourceToDisplay(String source) {
        if (source == null) {
            return VdbMetadataConstants.SOURCE_LATTICE_DATA_SCIENCE;
        }

        boolean exist = VdbMetadataConstants.SOURCE_MAPPING.containsKey(source);
        if (exist) {
            return VdbMetadataConstants.SOURCE_MAPPING.get(source);
        } else {
            return VdbMetadataConstants.SOURCE_LATTICE_DATA_SCIENCE;
        }
    }

    private Table getEventTableFromModelId(String modelId) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        ModelSummary modelSummary = modelSummaryEntityMgr.findValidByModelId(modelId);
        if (modelSummary == null) {
            throw new RuntimeException(String.format("No such model summary with id %s", modelId));
        }
        String tableName = modelSummary.getEventTableName();
        if (tableName == null) {
            throw new RuntimeException(String.format("Model %s does not have an event tableName", modelId));
        }

        Table table = metadataProxy.getTable(customerSpace, tableName);
        if (table == null) {
            throw new RuntimeException(String.format("No such table with name %s for model %s", tableName, modelId));
        }
        return table;
    }

    @Override
    public Table getTrainingTableFromModelId(String modelId) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        ModelSummary modelSummary = modelSummaryEntityMgr.findValidByModelId(modelId);
        if (modelSummary == null) {
            throw new RuntimeException(String.format("No such model summary with id %s", modelId));
        }
        String trainingTableName = modelSummary.getTrainingTableName();
        if (trainingTableName == null) {
            throw new RuntimeException(String.format("Model %s does not have an training table name", modelId));
        }

        Table table = metadataProxy.getTable(customerSpace, trainingTableName);
        if (table == null) {
            throw new RuntimeException(String.format("No training table with name %s for model %s", trainingTableName,
                    modelId));
        }
        return table;
    }

}
