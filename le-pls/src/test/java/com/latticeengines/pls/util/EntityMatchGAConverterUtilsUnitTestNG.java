package com.latticeengines.pls.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

public class EntityMatchGAConverterUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testConvert() {
        S3ImportSystem importSystem = new S3ImportSystem();
        importSystem.setName("Default_System");
        importSystem.setDisplayName("Default_System");
        importSystem.setAccountSystemId("Default_System_someID_AccountId");


        FieldMapping fm1 = new FieldMapping();
        fm1.setUserField("id1");
        fm1.setMappedField("AccountId");
        fm1.setFieldType(UserDefinedType.TEXT);
        FieldMapping fm2 = new FieldMapping();
        fm2.setUserField("id");
        fm2.setMappedField("SomeID");
        fm2.setFieldType(UserDefinedType.TEXT);
        FieldMapping fm3 = new FieldMapping();
        fm3.setUserField("SomethingElse");
        fm3.setMappedField("Default_System_someID_AccountId");
        fm3.setFieldType(UserDefinedType.TEXT);
        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        fieldMappingDocument.setFieldMappings(new ArrayList<>(Arrays.asList(fm1, fm2, fm3)));

        EntityMatchGAConverterUtils.convertSavingMappings(false, true, fieldMappingDocument, importSystem);
        Optional<FieldMapping> customerAccountIdMapping = fieldMappingDocument.getFieldMappings().stream()
                .filter(fieldMapping -> InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField()))
                .findAny();
        Assert.assertTrue(customerAccountIdMapping.isPresent());

        Optional<FieldMapping> customerContactIdMapping = fieldMappingDocument.getFieldMappings().stream()
                .filter(fieldMapping -> InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField()))
                .findAny();
        Assert.assertFalse(customerContactIdMapping.isPresent());

        Optional<FieldMapping> systemAccoundIdMapping = fieldMappingDocument.getFieldMappings().stream()
                .filter(fieldMapping -> "Default_System_someID_AccountId".equals(fieldMapping.getMappedField()))
                .findAny();
        Assert.assertTrue(systemAccoundIdMapping.isPresent());
        Assert.assertEquals(systemAccoundIdMapping.get().getUserField(), "id1");
    }

    @Test(groups = "unit")
    public void testConvertWithOutSystemIdMapping() {
        S3ImportSystem importSystem = new S3ImportSystem();
        importSystem.setName("Default_System");
        importSystem.setDisplayName("Default_System");
        importSystem.setAccountSystemId("Default_System_someID_AccountId");


        FieldMapping fm1 = new FieldMapping();
        fm1.setUserField("id1");
        fm1.setMappedField("AccountId");
        fm1.setFieldType(UserDefinedType.TEXT);
        FieldMapping fm2 = new FieldMapping();
        fm2.setUserField("id");
        fm2.setMappedField("SomeID");
        fm2.setFieldType(UserDefinedType.TEXT);
        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        fieldMappingDocument.setFieldMappings(new ArrayList<>(Arrays.asList(fm1, fm2)));

        EntityMatchGAConverterUtils.convertSavingMappings(false, true, fieldMappingDocument, importSystem);
        Optional<FieldMapping> customerAccountIdMapping = fieldMappingDocument.getFieldMappings().stream()
                .filter(fieldMapping -> InterfaceName.CustomerAccountId.name().equals(fieldMapping.getMappedField()))
                .findAny();
        Assert.assertTrue(customerAccountIdMapping.isPresent());

        Optional<FieldMapping> customerContactIdMapping = fieldMappingDocument.getFieldMappings().stream()
                .filter(fieldMapping -> InterfaceName.CustomerContactId.name().equals(fieldMapping.getMappedField()))
                .findAny();
        Assert.assertFalse(customerContactIdMapping.isPresent());

        Optional<FieldMapping> systemAccoundIdMapping = fieldMappingDocument.getFieldMappings().stream()
                .filter(fieldMapping -> "Default_System_someID_AccountId".equals(fieldMapping.getMappedField()))
                .findAny();
        Assert.assertTrue(systemAccoundIdMapping.isPresent());
        Assert.assertEquals(systemAccoundIdMapping.get().getUserField(), "id1");
    }
}
