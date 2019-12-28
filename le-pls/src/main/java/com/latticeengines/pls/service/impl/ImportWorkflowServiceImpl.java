package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.util.ImportWorkflowSpecUtils;
import com.latticeengines.pls.service.DataFileProviderService;
import com.latticeengines.pls.service.ImportWorkflowService;
import com.latticeengines.proxy.exposed.core.ImportWorkflowSpecProxy;

@Component("importWorkflowService")
public class ImportWorkflowServiceImpl implements ImportWorkflowService {

    @Value("${aws.s3.bucket}")
    private String s3SpecBucket;

    @Value("${aws.import.specs.s3.folder}")
    private String s3Folder;

    @Inject
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;

    @Inject
    private DataFileProviderService dataFileProviderService;

    /**
     * 1. Basic Validation of Individual Spec
     *  Must be valid JSON.
     *  Must be deserializable into valid ImportWorkflowSpec.
     *  All required fields are set for each Attribute
     *  Can't have two FieldDefinitions with the same fieldNames.
     *  Make sure no duplicates in matchingColumnNames.
     * 2. Spec must be validated against prior version for same System Type and System Object
     *  Physical type of a FieldDefinition with same fieldName cannot be changed in later Specs, at least if there
     *  exists tenant data for this Spec.
     * 3. Specs for the same System Object across System Types must be consistent.
     *  Physical type of a FieldDefinition with the same fieldName cannot vary across Specs for the same System Object.
     * @param systemType
     * @param systemObject
     * @param specInputStream
     * @return
     * @throws Exception
     */
    @Override
    public ImportWorkflowSpec validateIndividualSpec(String systemType, String systemObject,
                                                     InputStream specInputStream, List<String> errors) throws Exception {

        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();

        ImportWorkflowSpec existingSpec = importWorkflowSpecProxy.getImportWorkflowSpec(customerSpace.toString(),
                systemType,
                systemObject);
        List<ImportWorkflowSpec> specList =
                importWorkflowSpecProxy.getSpecWithSameObjectExcludeTypeFromS3(customerSpace.toString(), systemType,
                        systemObject);
        Map<String, Map<String, FieldDefinition>> specWithSameObjectMap = new HashMap<>();
        specList.forEach(spec ->
                specWithSameObjectMap.put(spec.getSystemType(),
                        spec.getFieldDefinitionsRecordsMap().values().stream().flatMap(List::stream).collect(Collectors.toMap(FieldDefinition::getFieldName, e -> e))));

        Map<String, List<FieldDefinition>> currentFieldDefinitionsMap;
        if (existingSpec == null) {
            currentFieldDefinitionsMap = new HashMap<>();
        } else {
            currentFieldDefinitionsMap = existingSpec.getFieldDefinitionsRecordsMap();
        }
        Map<String, FieldDefinition> fieldNameToDefinition =
                currentFieldDefinitionsMap.values().stream().flatMap(List::stream).collect(Collectors.toMap(FieldDefinition::getFieldName, e -> e));
        ImportWorkflowSpec importSpec = null;
        if (specInputStream != null) {
            try {
                importSpec = JsonUtils.deserialize(specInputStream, ImportWorkflowSpec.class);
            } catch (Exception e) {
                errors.add("input file can't be deserializable into valid ImportWorkflowSpec.");
            }
            Preconditions.checkNotNull(importSpec);
            if (importSpec.equals(existingSpec)) {
                errors.add(String.format("input spec matches the existing spec with system type %s and system object " +
                        "%s", systemType, systemObject));
            }
            Map<String, List<FieldDefinition>> inputFieldDefinitionMap = importSpec.getFieldDefinitionsRecordsMap();
            Preconditions.checkNotNull(inputFieldDefinitionMap);
            Set<String> fieldNameSet = new HashSet<>();
            Set<String> columnNamesSet = new HashSet<>();
            for (Map.Entry<String, List<FieldDefinition>> entry : inputFieldDefinitionMap.entrySet()) {
                List<FieldDefinition> definitions = entry.getValue();
                for (FieldDefinition definition : definitions) {
                    String fieldName = definition.getFieldName();
                    // check duplicates for definition
                    if (StringUtils.isEmpty(fieldName)) {
                        errors.add("empty field name found");
                    } else if (!fieldNameSet.add(definition.getFieldName())) {
                        // duplicate
                        errors.add(String.format("field definitions have same field name %s", fieldName));
                    }
                    // check duplicates for matching column name
                    List<String> matchingColumnNames = definition.getMatchingColumnNames();
                    if (CollectionUtils.isEmpty(matchingColumnNames)) {
                        errors.add(String.format("empty matching columns for field %s", fieldName));
                    } else {
                        for (String name : matchingColumnNames) {
                            if (!columnNamesSet.add(name)) {
                                // duplicate
                                errors.add(String.format("duplicates found in matching column for field name %s", fieldName));
                            }
                        }
                    }
                    // check required flag
                    if (definition.isRequired() == null) {
                        errors.add(String.format("required flag should be set for %s", fieldName));
                    }
                    // check field type
                    if (definition.getFieldType() == null) {
                        errors.add(String.format("field type is empty for field %s", fieldName));
                    }
                    FieldDefinition existingDefinition = fieldNameToDefinition.get(fieldName);
                    //TODO(penglong) to handle checking if there is existing tenant data before disallowing changes to
                    // the existing definition's fieldTypes
                    if (existingDefinition != null && definition.getFieldType() != existingDefinition.getFieldType()) {
                        // error out
                        errors.add(String.format("Physical type %s of the FieldDefinition with " +
                                        "same field name %s cannot be changed to %s for system type %s and system object " +
                                        "%s",
                                existingDefinition.getFieldType(),
                                fieldName, definition.getFieldType(),
                                systemType, systemObject));
                    }
                    for (Map.Entry<String, Map<String, FieldDefinition>> specEntry :
                            specWithSameObjectMap.entrySet()) {
                        Map<String, FieldDefinition> specMap = specEntry.getValue();
                        FieldDefinition specDefinition = specMap.get(fieldName);
                        if (specDefinition != null && definition.getFieldType() != specDefinition.getFieldType()) {
                            // error out
                            errors.add(String.format("Physical type %s of the field name %s in the current " +
                                            "systemType %s and systemObject %s cannot be different than the Physical " +
                                            "type %s in other template with system type %s and system object %s",
                                    definition.getFieldType(),
                                    fieldName, systemType.toLowerCase(), systemObject.toLowerCase(), specDefinition.getFieldType(),
                                    specEntry.getKey().toLowerCase(), systemObject.toLowerCase()));
                        }
                    }
                }
            }
        }

        return importSpec;
    }

    @Override
    public String uploadIndividualSpec(String systemType, String systemObject, InputStream inputStream) throws Exception {
        List<String> errors = new ArrayList<>();
        ImportWorkflowSpec importWorkflowSpec = validateIndividualSpec(systemType, systemObject, inputStream, errors);
        if (CollectionUtils.isEmpty(errors) && importWorkflowSpec != null) {
            importWorkflowSpecProxy.addSpecToS3(MultiTenantContext.getShortTenantId(), systemType, systemObject,
                    importWorkflowSpec);
            return "uploaded to S3 successfully.";
        } else {
            return String.join("\n", errors);
        }
    }

    @Override
    public void downloadSpecFromS3(HttpServletRequest request, HttpServletResponse response, String mimeType, String systemType, String systemObject) throws IOException {
        String specName = ImportWorkflowSpecUtils.constructSpecName(systemType, systemObject);
        String key = s3Folder + "/" + specName;
        dataFileProviderService.downloadS3File(request, response, mimeType, specName, key, s3SpecBucket);
    }

    @Override
    public List<ImportWorkflowSpec> getSpecsByTypeAndObject(String customerSpace, String systemType, String systemObject) {
        return importWorkflowSpecProxy.getSpecsByTypeAndObject(customerSpace, systemType, systemObject);
    }
}
