package com.latticeengines.domain.exposed.modeling;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class DataSchemaUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() throws Exception {
        DataSchema schema = new DataSchema();
        schema.setName("IrisDataSet");
        schema.setType("record");
        Field sepalLength = new Field();
        sepalLength.setName("sepal_length");
        sepalLength.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field sepalWidth = new Field();
        sepalWidth.setName("sepal_width");
        sepalWidth.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field petalLength = new Field();
        petalLength.setName("petal_length");
        petalLength.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field petalWidth = new Field();
        petalWidth.setName("petal_width");
        petalWidth.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field category = new Field();
        category.setName("category");
        category.setType(Arrays.<String> asList(new String[] { "string", "null" }));

        schema.addField(sepalLength);
        schema.addField(sepalWidth);
        schema.addField(petalLength);
        schema.addField(petalWidth);
        schema.addField(category);

        String jsonString = schema.toString();
        DataSchema deserializedSchema = JsonUtils.deserialize(jsonString, DataSchema.class);
        assertEquals(deserializedSchema.toString(), jsonString);
    }
}
