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

    @Test(groups = "unit")
    public void testSerDeIgnoresUnknownProperties() throws Exception {
        String contents = "{\"type\":\"record\",\"name\":\"EventTable\",\"doc\":\"GeneratedbydedupEventTable\",\"fields\":[{\"name\":\"AccountSource\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"52222f14-16e6-4f48-956f-1381da5413fe\",\"displayName\":\"AccountSource\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},"
                + "{\"name\":\"IsWon\",\"type\":[\"boolean\",\"null\"],\"logicalType\":\"BOOLEAN\",\"uuid\":\"a3fd0948-6fb4-4fee-81bf-d01ab4352cc7\",\"displayName\":\"IsWon\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"Street\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"fd85ca47-93fa-4fab-9e94"
                + "-0dfc45dcb5d6\",\"displayName\":\"Street\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"TickerSymbol\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"095f4b66-ea76-4732-8610-a029f1d02f2f\",\"displayName\":\"TickerSymbol\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"nam"
                + "e\":\"Website\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"22bdc8a0-91ca-4f4f-9396-95a2447a69bb\",\"displayName\":\"Website\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"Type\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"2f467a77-7d15-4b58-a610-2e90"
                + "ceb32bb8\",\"displayName\":\"Type\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"OwnerId\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"a1517872-98a4-493b-9005-c78fd6ab22bc\",\"displayName\":\"OwnerId\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"LastModifiedD"
                + "ate\",\"type\":[\"long\",\"null\"],\"logicalType\":\"LONG\",\"uuid\":\"1f36dda3-e6f8-443d-afb1-ad6358e5843a\",\"displayName\":\"LastModifiedDate\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"Country\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"7a9c8523-a9b1-4ce2-b86a-cdc8"
                + "bdea71a6\",\"displayName\":\"Country\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"AnnualRevenue\",\"type\":[\"double\",\"null\"],\"logicalType\":\"DOUBLE\",\"uuid\":\"008a1f6e-dead-42e8-9b9d-96a41f220f99\",\"displayName\":\"AnnualRevenue\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\""
                + ":\"City\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"3e99aa64-ce85-48b7-a8d5-65eac728c1dd\",\"displayName\":\"City\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"LastActivityDate\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"a54c2431-5518-4327-a71a-"
                + "ca3db2645232\",\"displayName\":\"LastActivityDate\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"Name\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"2b7dbb8b-f6bd-4505-aab5-476d5545d50f\",\"displayName\":\"Name\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"Num"
                + "berOfEmployees\",\"type\":[\"int\",\"null\"],\"logicalType\":\"INT\",\"uuid\":\"4915009a-6156-473f-a07d-20c75d853cac\",\"displayName\":\"NumberOfEmployees\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"LastViewedDate\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"f54dc6ed-20"
                + "c8-485a-92fe-01aa3e4f938c\",\"displayName\":\"LastViewedDate\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"PostalCode\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"e295603a-da03-4ae1-bfac-e88fee315c16\",\"displayName\":\"PostalCode\",\"ApprovedUsage\":\"[ModelAndAllI"
                + "nsights]\"},{\"name\":\"State\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"82639fca-a0e3-45c8-9cad-c24a12786fc1\",\"displayName\":\"State\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"Rating\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"c9729626-55c7-"
                + "4b90-98f8-3cea177a47cc\",\"displayName\":\"Rating\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"CreatedDate\",\"type\":[\"long\",\"null\"],\"logicalType\":\"LONG\",\"uuid\":\"55f3004c-dd13-408f-bd42-b7d65903d90c\",\"displayName\":\"CreatedDate\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\""
                + "name\":\"Id\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"6ce1fa49-4e69-4a36-9738-6e62d95ae9a7\",\"displayName\":\"Id\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"Ownership\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"869e605f-aecb-4f7d-be57-fa20d4"
                + "7953da\",\"displayName\":\"Ownership\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"Sic\",\"type\":[\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"212e3da9-8523-4883-af02-5699a31e78fb\",\"displayName\":\"Sic\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"},{\"name\":\"Industry\",\"type\":["
                + "\"string\",\"null\"],\"logicalType\":\"STRING\",\"uuid\":\"a943b31e-4fa7-4db8-99a3-fb8ffec111a6\",\"displayName\":\"Industry\",\"ApprovedUsage\":\"[ModelAndAllInsights]\"}],\"uuid\":\"df61c377-a746-40de-8a40-0540d5cd8e57\"}";

        JsonUtils.deserialize(contents, DataSchema.class);
    }
}
