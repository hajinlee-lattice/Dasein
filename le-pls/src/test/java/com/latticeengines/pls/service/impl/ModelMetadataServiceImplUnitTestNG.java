package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;

public class ModelMetadataServiceImplUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(ModelMetadataServiceImplUnitTestNG.class);

    private ModelMetadataServiceImpl modelMetadataService = new ModelMetadataServiceImpl();

    @Test(groups = "unit", enabled = false)
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

        List<Attribute> attributes = Arrays
                .<Attribute>asList(new Attribute[] { a1, a2, a3, a4, a5, a6, a7, a8, a9, a10 });
        Table t = new Table();
        t.setAttributes(attributes);
        List<Attribute> requiredColumns = pythonScriptModelService.getRequiredColumns(t);
        System.out.println(requiredColumns);
        assertEquals(requiredColumns.size(), 4);
        assertEquals(requiredColumns.get(0), a1);
        assertEquals(requiredColumns.get(1), a2);
        assertEquals(requiredColumns.get(2), a7);
        assertEquals(requiredColumns.get(3), a9);
    }

    @Test(groups = "unit")
    public void checkCascadingMetadata() {
        ObjectMapper mapper = new ObjectMapper();
        List<Attribute> attributes = new ArrayList<>();
        List<VdbMetadataField> fields = new ArrayList<>();

        Attribute attA = new Attribute();
        attA.setName("A");
        attA.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attA.setTags(Tag.INTERNAL);
        attributes.add(attA);

        VdbMetadataField fieldA = new VdbMetadataField();
        fieldA.setColumnName("A");
        fieldA.setApprovedUsage(ApprovedUsage.NONE.toString());
        fields.add(fieldA);

        Attribute attB = new Attribute();
        attB.setName("B");
        attB.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attB.setTags(Tag.INTERNAL);
        attributes.add(attB);

        VdbMetadataField fieldB = new VdbMetadataField();
        fieldB.setColumnName("B");
        fieldB.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        fields.add(fieldB);

        Attribute attC = new Attribute();
        attC.setName("C");
        attC.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attC.setTags(Tag.INTERNAL);
        attributes.add(attC);

        VdbMetadataField fieldC = new VdbMetadataField();
        fieldC.setColumnName("C");
        fieldC.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        fields.add(fieldC);

        Attribute attD = new Attribute();
        attD.setName("D");
        attD.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attD.setTags(Tag.INTERNAL_TRANSFORM);
        Map<String, Object> argsD = new HashMap<>();
        argsD.put("column1", "A");
        argsD.put("column2", "B");
        try {
            attD.setRTSArguments(mapper.writeValueAsString(argsD));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        attributes.add(attD);

        VdbMetadataField fieldD = new VdbMetadataField();
        fieldD.setColumnName("D");
        fieldD.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        fields.add(fieldD);

        Attribute attE = new Attribute();
        attE.setName("E");
        attE.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attE.setTags(Tag.INTERNAL_TRANSFORM);
        Map<String, Object> argsE = new HashMap<>();
        argsE.put("column", "C");
        try {
            attE.setRTSArguments(mapper.writeValueAsString(argsE));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        attributes.add(attE);

        VdbMetadataField fieldE = new VdbMetadataField();
        fieldE.setColumnName("E");
        fieldE.setApprovedUsage(ApprovedUsage.NONE.toString());
        fields.add(fieldE);

        Attribute attF = new Attribute();
        attF.setName("F");
        attF.setApprovedUsage(ApprovedUsage.NONE);
        attF.setTags(Tag.INTERNAL_TRANSFORM);
        Map<String, Object> argsF = new HashMap<>();
        argsF.put("column", "D");
        try {
            attF.setRTSArguments(mapper.writeValueAsString(argsF));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        attributes.add(attF);

        VdbMetadataField fieldF = new VdbMetadataField();
        fieldF.setColumnName("F");
        fieldF.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        fields.add(fieldF);

        Attribute attG = new Attribute();
        attG.setName("G");
        attG.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attG.setTags(Tag.INTERNAL_TRANSFORM);
        Map<String, Object> argsG = new HashMap<>();
        argsG.put("column1", "D");
        argsG.put("column2", "E");
        try {
            attG.setRTSArguments(mapper.writeValueAsString(argsG));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        attributes.add(attG);

        VdbMetadataField fieldG = new VdbMetadataField();
        fieldG.setColumnName("G");
        fieldG.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        fields.add(fieldG);

        Attribute attH = new Attribute();
        attH.setName("H");
        attH.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attH.setTags(Tag.EXTERNAL);
        attributes.add(attH);

        VdbMetadataField fieldH = new VdbMetadataField();
        fieldH.setColumnName("H");
        fieldH.setApprovedUsage(ApprovedUsage.NONE.toString());
        fields.add(fieldH);

        Attribute attI = new Attribute();
        attI.setName("I");
        attI.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attI.setTags(Tag.EXTERNAL);
        attributes.add(attI);

        VdbMetadataField fieldI = new VdbMetadataField();
        fieldI.setColumnName("I");
        fieldI.setApprovedUsage(ApprovedUsage.NONE.toString());
        fields.add(fieldI);

        List<Attribute> updatedAttributes = modelMetadataService.getAttributesFromFields(attributes, fields);

        assertEquals(updatedAttributes.get(0).getApprovedUsage().get(0), ApprovedUsage.NONE.toString());
        assertEquals(updatedAttributes.get(1).getApprovedUsage().get(0), ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        assertEquals(updatedAttributes.get(2).getApprovedUsage().get(0), ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        assertEquals(updatedAttributes.get(3).getApprovedUsage().get(0), ApprovedUsage.NONE.toString());
        assertEquals(updatedAttributes.get(4).getApprovedUsage().get(0), ApprovedUsage.NONE.toString());
        assertEquals(updatedAttributes.get(5).getApprovedUsage().get(0), ApprovedUsage.MODEL_ALLINSIGHTS.toString());
        assertEquals(updatedAttributes.get(6).getApprovedUsage().get(0), ApprovedUsage.NONE.toString());
        assertEquals(updatedAttributes.get(7).getApprovedUsage().get(0), ApprovedUsage.NONE.toString());
        assertEquals(updatedAttributes.get(8).getApprovedUsage().get(0), ApprovedUsage.NONE.toString());

    }
}
