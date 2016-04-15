package com.latticeengines.pls.service.impl.fileprocessor;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.VdbMetadataConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("ModelMetadataService")
public class ModelMetadataServiceImpl implements ModelMetadataService {

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Override
    public List<VdbMetadataField> getMetadata(String modelId) {
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

        List<VdbMetadataField> fields = new ArrayList<>();
        for (Attribute attribute : table.getAttributes()) {
            VdbMetadataField field = getFieldFromAttribute(attribute);
            fields.add(field);
        }
        return fields;
    }

    @Override
    public Table cloneAndUpdateMetadata(String modelSummaryId, List<VdbMetadataField> fields) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(modelSummaryId);
        String eventTableName = summary.getEventTableName();
        Table eventTable = metadataProxy.getTable(customerSpace, eventTableName);
        if (eventTable == null) {
            throw new RuntimeException(String.format("Could not find event table with name %s", eventTableName));
        }
        Table clone = metadataProxy.cloneTable(customerSpace, eventTable.getName());
        clone.setAttributes(getAttributesFromFields(clone.getAttributes(), fields));
        metadataProxy.updateTable(customerSpace, clone.getName(), clone);
        return clone;
    }

    @Override
    public List<String> getRequiredColumns(String modelId) {
        List<String> requiredColumns = new ArrayList<String>();
        ModelSummary modelSummary = modelSummaryEntityMgr.findValidByModelId(modelId);
        if (modelSummary == null) {
            throw new RuntimeException(String.format("No such model summary with id %s", modelId));
        }
        List<Predictor> predictors = modelSummary.getPredictors();
        if (predictors == null) {
            throw new RuntimeException(String.format("No predictor is associated with the model %s", modelId));
        }
        for (Predictor predictor : predictors) {
            if (predictor.getCategory().contains(ModelingMetadata.INTERNAL_TAG)) {
                requiredColumns.add(predictor.getName());
            }
        }
        return requiredColumns;
    }

    private List<Attribute> getAttributesFromFields(List<Attribute> attributes, List<VdbMetadataField> fields) {
        List<Attribute> attributeCopy = new ArrayList<>(attributes);
        List<Attribute> editedAttributes = new ArrayList<>();

        for (Attribute attribute : attributes) {
            for (VdbMetadataField field : fields) {
                if (attribute.getName().equals(field.getColumnName())) {
                    attributeCopy.remove(attribute);
                    editedAttributes.add(overwriteAttributeWithFieldValues(attribute, field));
                }
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

}
