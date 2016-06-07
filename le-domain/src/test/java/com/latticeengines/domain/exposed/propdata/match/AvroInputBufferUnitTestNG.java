package com.latticeengines.domain.exposed.propdata.match;

import com.latticeengines.common.exposed.util.JsonUtils;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AvroInputBufferUnitTestNG {

    @Test(groups = "unit")
    public void testParseSchema() throws Exception {
        Schema schema = new Schema.Parser().parse("{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"SourceFile_csv\",\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"Source_Name\",\n" +
                "    \"type\" : [ \"string\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"Source_State\",\n" +
                "    \"type\" : [ \"string\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"Source_Domain\",\n" +
                "    \"type\" : [ \"string\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"Source_ID\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"Source_Country\",\n" +
                "    \"type\" : [ \"string\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"Source_City\",\n" +
                "    \"type\" : [ \"string\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"ActiveRetirementParticipants\",\n" +
                "    \"type\" : [ \"int\", \"null\" ],\n" +
                "    \"Tags\" : \"External\",\n" +
                "    \"Description\" : \"Number of participants in the company reitirement plan that are in active service with the company\",\n" +
                "    \"Category\" : \"Firmographics\",\n" +
                "    \"FundamentalType\" : \"numeric\",\n" +
                "    \"StatisticalType\" : \"ratio\",\n" +
                "    \"DisplayName\" : \"Active Retirement Plan Participants\",\n" +
                "    \"DiscretizationStrategy\" : \"{\\\"geometric\\\": { \\\"minValue\\\":10,\\\"multiplierList\\\":[2,2.5,2],\\\"minSamples\\\":100,\\\"minFreq\\\":0.01,\\\"maxBuckets\\\":5,\\\"maxPercentile\\\":1}}\",\n" +
                "    \"ApprovedUsage\" : \"Model\"\n" +
                "  }] }");

        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setSchema(null);
        inputBuffer.setAvroDir("/tmp");
        AvroInputBuffer inputBuffer2 = JsonUtils.deserialize(JsonUtils.serialize(inputBuffer), AvroInputBuffer.class);

        inputBuffer = new AvroInputBuffer();
        inputBuffer.setSchema(schema);
        inputBuffer.setAvroDir("/tmp");
        inputBuffer2 = JsonUtils.deserialize(JsonUtils.serialize(inputBuffer), AvroInputBuffer.class);

        Assert.assertEquals(inputBuffer2.getSchema().getFields().size(), 7);
    }

}
