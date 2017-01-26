package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.Field;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.Fields;

public class ModelRetrieverUnitTestNG {

    @Test(groups = "unit")
    public void testRemoveDroppedDataScienceFieldEventTableTransforms() throws IOException {
        URL eventTableDataCompositionUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoringapi/model/eventtable-datacomposition.json");
        String eventTableDataCompositionContents = Files.toString(new File(eventTableDataCompositionUrl.getFile()),
                Charset.defaultCharset());
        DataComposition eventTableDataComposition = JsonUtils.deserialize(eventTableDataCompositionContents,
                DataComposition.class);

        URL dataScienceDataCompositionUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoringapi/model/datascience-datacomposition.json");
        String dataScienceDataCompositionContents = Files.toString(new File(dataScienceDataCompositionUrl.getFile()),
                Charset.defaultCharset());
        DataComposition dataScienceDataComposition = JsonUtils.deserialize(dataScienceDataCompositionContents,
                DataComposition.class);

        ModelRetrieverImpl modelRetriever = new ModelRetrieverImpl();
        Map<String, FieldSchema> mergedFields = modelRetriever.mergeFields(eventTableDataComposition,
                dataScienceDataComposition);

        Assert.assertEquals(mergedFields.size(), 349);
    }

    @Test(groups = "unit")
    public void testSetField() {
        ModelRetrieverImpl modelRetriever = new ModelRetrieverImpl();
        List<Field> fieldList = new ArrayList<>();
        String requiredColumnName = "";

        String[] fieldNameArr = new String[] { "Id", "Website", "Email", "Random" };
        FieldSchema fieldSchema = new FieldSchema(FieldSource.REQUEST, //
                FieldType.STRING, FieldInterpretation.Id);

        SchemaInterpretation schemaInterpretation = SchemaInterpretation.SalesforceAccount;
        requiredColumnName = InterfaceName.Website.name();
        boolean fuzzyMatchEnabled = false;
        for (String fieldName : fieldNameArr) {
            modelRetriever.setField(fieldList, fieldName, fieldName, fieldSchema, schemaInterpretation,
                    requiredColumnName, fuzzyMatchEnabled);
        }
        Assert.assertTrue(fieldList.get(0).isRequiredForScoring());
        Assert.assertTrue(fieldList.get(1).isRequiredForScoring());
        Assert.assertFalse(fieldList.get(2).isRequiredForScoring());
        Assert.assertFalse(fieldList.get(3).isRequiredForScoring());
        fieldList.clear();

        fuzzyMatchEnabled = true;
        for (String fieldName : fieldNameArr) {
            modelRetriever.setField(fieldList, fieldName, fieldName, fieldSchema, schemaInterpretation,
                    requiredColumnName, fuzzyMatchEnabled);
        }
        Assert.assertTrue(fieldList.get(0).isRequiredForScoring());
        Assert.assertFalse(fieldList.get(1).isRequiredForScoring());
        Assert.assertFalse(fieldList.get(2).isRequiredForScoring());
        Assert.assertFalse(fieldList.get(3).isRequiredForScoring());
        fieldList.clear();

        schemaInterpretation = SchemaInterpretation.SalesforceLead;
        requiredColumnName = InterfaceName.Email.name();
        fuzzyMatchEnabled = false;
        for (String fieldName : fieldNameArr) {
            modelRetriever.setField(fieldList, fieldName, fieldName, fieldSchema, schemaInterpretation,
                    requiredColumnName, fuzzyMatchEnabled);
        }
        Assert.assertTrue(fieldList.get(0).isRequiredForScoring());
        Assert.assertFalse(fieldList.get(1).isRequiredForScoring());
        Assert.assertTrue(fieldList.get(2).isRequiredForScoring());
        Assert.assertFalse(fieldList.get(3).isRequiredForScoring());
        fieldList.clear();

        fuzzyMatchEnabled = true;
        for (String fieldName : fieldNameArr) {
            modelRetriever.setField(fieldList, fieldName, fieldName, fieldSchema, schemaInterpretation,
                    requiredColumnName, fuzzyMatchEnabled);
        }
        Assert.assertTrue(fieldList.get(0).isRequiredForScoring());
        Assert.assertFalse(fieldList.get(1).isRequiredForScoring());
        Assert.assertFalse(fieldList.get(2).isRequiredForScoring());
        Assert.assertFalse(fieldList.get(3).isRequiredForScoring());
    }

    @Test(groups = "unit")
    public void test() {
        Fields fields = new Fields();
        List<Field> fieldList = new ArrayList<Field>();
        fields.setFields(fieldList);
        Field f1 = new Field("f1", FieldType.STRING, "f1");
        Field f2 = new Field("f2", FieldType.STRING, "f2");
        Field f3 = new Field("f3", FieldType.STRING, "f3");
        f1.setPrimaryField(false);
        f1.setRequiredForScoring(false);
        f2.setRequiredForScoring(true);
        f3.setRequiredForScoring(true);
        fieldList.add(f1);
        fieldList.add(f2);
        fieldList.add(f3);
        fields.setModelId("modelId");
        System.out.println(JsonUtils.serialize(fields));
    }

}
