package com.latticeengines.domain.exposed.workflow;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.JsonUtils;

public class KeyValueUnitTestNG {

    @Test(groups= {"unit"})
    public void testPayload() throws IOException {
        KeyValue keyValue = new KeyValue();
        String jsonSrc = "{\"Field\":\"value\",\"ID\":2}";
        keyValue.setData(CompressionUtils.compressByteArray(jsonSrc.getBytes()));
        String payload = JsonUtils.deserialize(JsonUtils.serialize(keyValue), KeyValue.class).getPayload();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(payload);
        Assert.assertEquals(jsonNode.get("Field").asText(), "value");
        Assert.assertEquals(jsonNode.get("ID").asInt(), 2);
    }

    @Test(groups= {"unit"})
    public void testReport() throws IOException {
        KeyValue keyValue = new KeyValue();
        String jsonSrc = "{\"Field\":\"value\",\"ID\":2}";
        keyValue.setData(CompressionUtils.compressByteArray(jsonSrc.getBytes()));

        Report report = new Report();
        report.setJson(keyValue);

        System.out.println(JsonUtils.deserialize(JsonUtils.serialize(report), Report.class).toString());
    }

}
