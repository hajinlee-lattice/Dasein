package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelService;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.VdbMetadataConstants;
import com.latticeengines.pls.util.MetadataUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("modelMetadataService")
public class ModelMetadataServiceImpl implements ModelMetadataService {

    private static final Logger log = LoggerFactory.getLogger(ModelMetadataServiceImpl.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    private ModelService getRequiredColumnsExtractor(String modelId) {
        ModelSummary modelSummary = modelSummaryEntityMgr.findByModelId(modelId, false, false, true);
        String modelTypeStr = modelSummary != null ? modelSummary.getModelType() : ModelType.PYTHONMODEL.getModelType();
        return ModelServiceBase.getModelService(modelTypeStr);
    }

    @Override
    public List<VdbMetadataField> getMetadata(String modelId) {
        Table table = getEventTableFromModelId(modelId);
        List<VdbMetadataField> fields = new ArrayList<>();
        for (Attribute attribute : table.getAttributes()) {
            if (!attribute.getName().equals(InterfaceName.InternalId.name())) {
                VdbMetadataField field = getFieldFromAttribute(attribute);
                fields.add(field);
            }
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
        return getRequiredColumnsExtractor(modelId).getRequiredColumnDisplayNames(modelId);
    }

    @Override
    public List<Attribute> getRequiredColumns(String modelId) {
        return getRequiredColumnsExtractor(modelId).getRequiredColumns(modelId);
    }

    @Override
    public Set<String> getLatticeAttributeNames(String modelId) {
        return getRequiredColumnsExtractor(modelId).getLatticeAttributeNames(modelId);
    }

    @Override
    public List<Attribute> getAttributesFromFields(List<Attribute> attributes, List<VdbMetadataField> fields) {
        List<Attribute> attributeCopy = new ArrayList<>(attributes);
        List<Attribute> editedAttributes = new ArrayList<>();

        // When the user sets ApprovedUsage to None on an attribute, propagate
        // that setting to derived attributes. However, we do not go the other
        // way: when setting ApprovedUsage to something other than None, do not
        // propagate.
        List<Attribute> userEditedAttributes = new ArrayList<>();
        Map<Attribute, List<Attribute>> parentChild = new HashMap<>();
        Map<Attribute, List<Attribute>> childParent = new HashMap<>();

        for (VdbMetadataField field : fields) {
            boolean found = false;
            for (Attribute attribute : attributes) {
                if (attribute.getName().equals(field.getColumnName())) {
                    ApprovedUsage approvedUsageOriginal = ApprovedUsage.fromName(attribute.getApprovedUsage().get(0));
                    attributeCopy.remove(attribute);
                    Attribute updatedAttribute = overwriteAttributeWithFieldValues(attribute, field);
                    ApprovedUsage approvedUsageUpdated = ApprovedUsage
                            .fromName(updatedAttribute.getApprovedUsage().get(0));
                    if (approvedUsageUpdated != approvedUsageOriginal)
                        userEditedAttributes.add(updatedAttribute);
                    editedAttributes.add(updatedAttribute);
                    found = true;
                    break;
                }
                updateAttributeGenerationMaps(parentChild, childParent, attribute, attributes);
            }
            if (!found) {
                log.info(String.format("Not found field %s n in Attribute List.", field.getColumnName()));
                Attribute newAttribute = new Attribute();
                newAttribute.setName(field.getColumnName());
                overwriteAttributeWithFieldValues(newAttribute, field);
                attributeCopy.add(newAttribute);
            }
        }

        class CompareNumberOfGenerations implements Comparator<Attribute> {

            private Map<Attribute, Integer> generation = new HashMap<>();

            CompareNumberOfGenerations(Map<Attribute, List<Attribute>> childParent) {
                for (Map.Entry<Attribute, List<Attribute>> entry : childParent.entrySet()) {
                    generation.put(entry.getKey(), getMaxGeneration(childParent, entry.getValue()));
                }
            }

            private Integer getMaxGeneration(Map<Attribute, List<Attribute>> childParent, List<Attribute> attributes) {
                if (attributes == null || attributes.size() == 0)
                    return 0;
                Integer maxGenerationOfParents = 0;
                for (Attribute attribute : attributes) {
                    Integer parentGeneration = 0;
                    if (childParent.containsKey(attribute)) {
                        parentGeneration = getMaxGeneration(childParent, childParent.get(attribute));
                    }
                    if (parentGeneration > maxGenerationOfParents)
                        maxGenerationOfParents = parentGeneration;
                }
                return 1 + maxGenerationOfParents;
            }

            @Override
            public int compare(Attribute a1, Attribute a2) {
                Integer gen1 = generation.containsKey(a1) ? generation.get(a1) : 0;
                Integer gen2 = generation.containsKey(a2) ? generation.get(a2) : 0;
                return Integer.compare(gen1, gen2);
            }
        }

        Collections.sort(userEditedAttributes, new CompareNumberOfGenerations(childParent));

        for (Attribute userEditedAttribute : userEditedAttributes) {
            if (parentChild.containsKey(userEditedAttribute))
                setApprovedUsageNoneRecursively(parentChild.get(userEditedAttribute), userEditedAttributes,
                        parentChild);

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
        field.setIsCoveredByMandatoryRule(attribute.getIsCoveredByMandatoryRule());
        field.setIsCoveredByOptionalRule(attribute.getIsCoveredByOptionalRule());
        field.setAssociatedRules(attribute.getAssociatedDataRules());

        return field;
    }

    private void updateAttributeGenerationMaps(Map<Attribute, List<Attribute>> parentChild,
            Map<Attribute, List<Attribute>> childParent, Attribute childAttribute, List<Attribute> allAttributes) {
        for (String parentName : childAttribute.getParentAttributeNames()) {
            Attribute parent = null;
            for (Attribute attributeCandidate : allAttributes) {
                if (attributeCandidate.getName().equals(parentName)) {
                    parent = attributeCandidate;
                    break;
                }
            }
            if (parent != null) {
                if (!parentChild.containsKey(parent)) {
                    parentChild.put(parent, new ArrayList<Attribute>());
                }
                if (!childParent.containsKey(childAttribute)) {
                    childParent.put(childAttribute, new ArrayList<Attribute>());
                }
                parentChild.get(parent).add(childAttribute);
                childParent.get(childAttribute).add(parent);
            }
        }
    }

    private void setApprovedUsageNoneRecursively(List<Attribute> attributes, List<Attribute> userEditedAttributes,
            Map<Attribute, List<Attribute>> parentChild) {
        for (Attribute attribute : attributes) {
            boolean isUserEdited = false;
            for (Attribute userEditedAttribute : userEditedAttributes) {
                if (userEditedAttribute.getName().equals(attribute.getName())) {
                    isUserEdited = true;
                    break;
                }
            }
            if (!isUserEdited)
                attribute.setApprovedUsage(ApprovedUsage.NONE);
            if (parentChild.containsKey(attribute)) {
                setApprovedUsageNoneRecursively(parentChild.get(attribute), userEditedAttributes, parentChild);
            }
        }
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

    @Override
    public Table getEventTableFromModelId(String modelId) {
        return MetadataUtils.getEventTableFromModelId(modelId, modelSummaryEntityMgr, metadataProxy);
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
            throw new RuntimeException(
                    String.format("No training table with name %s for model %s", trainingTableName, modelId));
        }
        return table;
    }

}
