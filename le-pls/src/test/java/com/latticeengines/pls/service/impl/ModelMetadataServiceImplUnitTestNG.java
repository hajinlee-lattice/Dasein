package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class ModelMetadataServiceImplUnitTestNG {

    @Test(groups = "unit")
    public void getRequiredColumnsFromPythonScriptModel() {
        PythonScriptModelService pythonScriptModelService = new PythonScriptModelService();

        Attribute a1 = new Attribute();
        a1.setName(InterfaceName.Id.name());
        a1.setDisplayName("Lead ID");
        a1.setTags(Tag.INTERNAL);
        a1.setApprovedUsage(ApprovedUsage.NONE);

        Attribute a2 = new Attribute();
        a2.setName(InterfaceName.Country.name());
        a2.setDisplayName("Country Name");
        a2.setTags(Tag.EXTERNAL);
        a2.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);

        Attribute a3 = new Attribute();
        a3.setName("SomePropdataColumn");
        a3.setDisplayName("SomePropdataColumn");
        a3.setTags(Tag.EXTERNAL);
        a3.setApprovedUsage(ApprovedUsage.NONE);

        Attribute a4 = new Attribute();
        a4.setName("SomePropdataColumn2");
        a4.setDisplayName("SomePropdataColumn2");
        a4.setTags(Tag.EXTERNAL);
        a4.setApprovedUsage(ApprovedUsage.MODEL);

        Attribute a5 = new Attribute();
        a5.setName("SomeTransformationColumn");
        a5.setDisplayName("SomePropdataColumn");
        a5.setTags(Tag.INTERNAL_TRANSFORM);
        a5.setApprovedUsage(ApprovedUsage.MODEL);

        Attribute a6 = new Attribute();
        a6.setName("SomeTransformationColumn");
        a6.setDisplayName("SomeTransformationColumn");
        a6.setTags(Tag.EXTERNAL_TRANSFORM);
        a6.setApprovedUsage(ApprovedUsage.MODEL);

        Attribute a7 = new Attribute();
        a7.setName(InterfaceName.Id.name());
        a7.setDisplayName(InterfaceName.Id.name());
        a7.setApprovedUsage(ApprovedUsage.NONE);

        Attribute a8 = new Attribute();
        a8.setName(InterfaceName.Event.name());
        a8.setDisplayName(InterfaceName.Event.name());
        a8.setLogicalDataType(LogicalDataType.Event);
        a8.setTags(Tag.INTERNAL);
        a8.setApprovedUsage(ApprovedUsage.NONE);

        Attribute a9 = new Attribute();
        a9.setName("SomeCustomerFeatureColumn");
        a9.setDisplayName("Some Customer Feature Column");
        a9.setTags(Tag.INTERNAL);
        a9.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);

        Attribute a10 = new Attribute();
        a10.setName("SomeCustomerColumn");
        a10.setDisplayName("Some Customer Column");
        a10.setTags(Tag.INTERNAL);
        a10.setApprovedUsage(ApprovedUsage.NONE);

        List<Attribute> attributes = Arrays.<Attribute> asList(new Attribute[] { a1, a2, a3, a4, a5, a6, a7, a8, a9,
                a10 });
        List<Attribute> requiredColumns = pythonScriptModelService.getRequiredColumns(attributes,
                SchemaInterpretation.SalesforceLead);
        System.out.println(requiredColumns);
        assertEquals(requiredColumns.size(), 4);
        assertEquals(requiredColumns.get(0), a1);
        assertEquals(requiredColumns.get(1), a2);
        assertEquals(requiredColumns.get(2), a7);
        assertEquals(requiredColumns.get(3), a9);
    }
}
