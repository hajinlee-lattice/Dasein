package com.latticeengines.domain.exposed.pls.frontend;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

public class FieldMappingDocumentUnitTestNG {

    @Test(groups = "unit")
    public void testDedup() {
        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        FieldMapping fieldMapping1 = new FieldMapping();
        fieldMapping1.setUserField("CustomerAttr1");
        fieldMapping1.setMappedField("user_Attr1");
        fieldMapping1.setFieldType(UserDefinedType.TEXT);
        fieldMapping1.setMappedToLatticeField(false);
        FieldMapping fieldMapping2 = new FieldMapping();
        fieldMapping2.setUserField("CustomerAttr1");
        fieldMapping2.setMappedField("user_Attr1");
        fieldMapping2.setFieldType(UserDefinedType.TEXT);
        fieldMapping2.setMappedToLatticeField(true);
        FieldMapping fieldMapping3 = new FieldMapping();
        fieldMapping3.setUserField("CustomerAttr1");
        fieldMapping3.setMappedField("user_Attr1");
        fieldMapping3.setFieldType(UserDefinedType.NUMBER);
        fieldMapping3.setMappedToLatticeField(false);
        FieldMapping fieldMapping4 = new FieldMapping();
        fieldMapping4.setUserField("CustomerAttr1");
        fieldMapping4.setFieldType(UserDefinedType.TEXT);
        fieldMapping4.setMappedToLatticeField(false);
        //duplicate with fieldMapping1
        FieldMapping fieldMapping5 = new FieldMapping();
        fieldMapping5.setUserField("CustomerAttr1");
        fieldMapping5.setMappedField("user_Attr1");
        fieldMapping5.setFieldType(UserDefinedType.TEXT);
        fieldMapping5.setMappedToLatticeField(false);

        FieldMapping fieldMapping6 = new FieldMapping();
        fieldMapping6.setUserField("CustomerAttr1");
        fieldMapping6.setMappedField("user_Attr1");
        fieldMapping6.setFieldType(UserDefinedType.TEXT);
        fieldMapping6.setCdlExternalSystemType(CDLExternalSystemType.CRM);
        fieldMapping6.setMappedToLatticeField(false);
        //duplicate with fieldMapping6
        FieldMapping fieldMapping7 = new FieldMapping();
        fieldMapping7.setUserField("CustomerAttr1");
        fieldMapping7.setMappedField("user_Attr1");
        fieldMapping7.setFieldType(UserDefinedType.TEXT);
        fieldMapping6.setCdlExternalSystemType(CDLExternalSystemType.CRM);
        fieldMapping7.setMappedToLatticeField(false);
        List<FieldMapping> fieldMappingList = new ArrayList<>(Arrays.asList(fieldMapping1, fieldMapping2, fieldMapping3, fieldMapping4,
                fieldMapping5, fieldMapping6, fieldMapping7));
        fieldMappingDocument.setFieldMappings(fieldMappingList);

        Assert.assertEquals(fieldMappingDocument.getFieldMappings().size(), 7);
        fieldMappingDocument.dedupFieldMappings();
        Assert.assertEquals(fieldMappingDocument.getFieldMappings().size(), 5);
    }
}
