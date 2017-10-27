package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

public class ScoringFileMetadataServiceImplUnitTestNG {

    private ScoringFileMetadataServiceImpl scoringFileMetadataServiceImpl = new ScoringFileMetadataServiceImpl();
    private static final String standardAttrName1 = "Email";
    private static final String standardAttrName2 = "CompanyName";
    private static final String userDefinedModelAttributeName = "userDefinedModelAttributeName";
    // userDefinedScoreAttributeMappedSchemaColumnName is in standard schema
    private static final String userDefinedScoreAttributeCSVHeaderName = "Primary Country";
    private static final String userDefinedScoreAttributeMappedSchemaColumnName = "Country";
    // userDefinedScoreAttributeName is NOT in standard schema
    private static final String userDefinedScoreAttributeName = "someRandomName";

    @Test(groups = "unit")
    public void testResolveModelAttributeBasedOnFieldMapping() {
        List<Attribute> modelAttributes = generateTestModelAttributes();
        // List<Attribute> schemaAttributes = generateTestSchemaAttributes();
        List<FieldMapping> fieldMappingList = generateTestFieldMappingDocument().getFieldMappings();
        scoringFileMetadataServiceImpl.resolveModelAttributeBasedOnFieldMapping(modelAttributes,
                generateTestFieldMappingDocument());
        Assert.assertEquals(modelAttributes.size(), fieldMappingList.size());
        assertUserDefinedAttributeInSchemaIsCorrectlySet(modelAttributes);
    }

    private void assertUserDefinedAttributeInSchemaIsCorrectlySet(List<Attribute> modelAttributes) {
        List<Attribute> countryAttr = modelAttributes.stream()
                .filter(attribute -> attribute.getDisplayName().equals(userDefinedScoreAttributeCSVHeaderName))
                .collect(Collectors.toList());
        Assert.assertTrue(countryAttr.size() == 1);
        Assert.assertEquals(countryAttr.get(0).getName(), userDefinedScoreAttributeMappedSchemaColumnName);
    }

    private List<Attribute> generateTestModelAttributes() {
        List<Attribute> modelAttributeList = new ArrayList<Attribute>();
        Attribute standardAttr1 = new Attribute();
        Attribute standardAttr2 = new Attribute();
        Attribute userDefinedModelAttribute = new Attribute();
        Attribute userDefinedScoreAttributeMappedSchemaAttribute = new Attribute();
        modelAttributeList.add(standardAttr1);
        modelAttributeList.add(standardAttr2);
        modelAttributeList.add(userDefinedModelAttribute);
        modelAttributeList.add(userDefinedScoreAttributeMappedSchemaAttribute);
        standardAttr1.setName(standardAttrName1);
        standardAttr1.setDisplayName(standardAttrName1);
        standardAttr2.setName(standardAttrName2);
        standardAttr2.setDisplayName(standardAttrName2);
        userDefinedModelAttribute.setName(userDefinedModelAttributeName);
        userDefinedModelAttribute.setDisplayName(userDefinedModelAttributeName);
        userDefinedScoreAttributeMappedSchemaAttribute.setName(userDefinedScoreAttributeMappedSchemaColumnName);
        userDefinedScoreAttributeMappedSchemaAttribute.setDisplayName(userDefinedScoreAttributeCSVHeaderName);

        return modelAttributeList;
    }

    private FieldMappingDocument generateTestFieldMappingDocument() {
        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        List<FieldMapping> fieldMappingList = new ArrayList<FieldMapping>();
        fieldMappingDocument.setFieldMappings(fieldMappingList);

        FieldMapping unMappedScoreHeaderField = new FieldMapping();
        unMappedScoreHeaderField.setFieldType(UserDefinedType.TEXT);
        unMappedScoreHeaderField.setMappedToLatticeField(false);
        unMappedScoreHeaderField.setUserField(userDefinedScoreAttributeName);

        FieldMapping mappedToUserProvidedRequiredColumnField = new FieldMapping();
        mappedToUserProvidedRequiredColumnField.setMappedToLatticeField(true);
        mappedToUserProvidedRequiredColumnField.setMappedField(userDefinedModelAttributeName);
        mappedToUserProvidedRequiredColumnField.setUserField(userDefinedModelAttributeName);

        FieldMapping mappedToModelRequiredColumnField1 = new FieldMapping();
        mappedToModelRequiredColumnField1.setUserField(standardAttrName1);
        mappedToModelRequiredColumnField1.setMappedField(standardAttrName1);
        mappedToModelRequiredColumnField1.setMappedToLatticeField(true);
        FieldMapping mappedToModelRequiredColumnField2 = new FieldMapping();
        mappedToModelRequiredColumnField2.setUserField(standardAttrName2);
        mappedToModelRequiredColumnField2.setMappedField(standardAttrName2);
        mappedToModelRequiredColumnField2.setMappedToLatticeField(true);

        FieldMapping mappedToSchemaColumnField = new FieldMapping();
        mappedToSchemaColumnField.setMappedToLatticeField(true);
        mappedToSchemaColumnField.setUserField(userDefinedScoreAttributeCSVHeaderName);
        mappedToSchemaColumnField.setMappedField(userDefinedScoreAttributeMappedSchemaColumnName);

        fieldMappingList.add(unMappedScoreHeaderField);
        fieldMappingList.add(mappedToUserProvidedRequiredColumnField);
        fieldMappingList.add(mappedToModelRequiredColumnField1);
        fieldMappingList.add(mappedToModelRequiredColumnField2);
        fieldMappingList.add(mappedToSchemaColumnField);

        fieldMappingDocument.setIgnoredFields(
                Arrays.asList(new String[] { "Website", "DUNS", "City", "State", "PhoneNumber", "PostalCode" }));

        return fieldMappingDocument;
    }
}
