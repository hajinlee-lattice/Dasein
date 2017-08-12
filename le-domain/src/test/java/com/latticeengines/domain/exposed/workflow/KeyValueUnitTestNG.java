package com.latticeengines.domain.exposed.workflow;

import java.io.IOException;

import com.fasterxml.jackson.databind.node.ObjectNode;
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
        String payload = keyValue.getPayload();
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

        System.out.println(report.toString());
    }

    @Test(groups= {"unit"})
    public void testReportWithEmptyPayload() throws IOException {
        ObjectNode json = JsonUtils.createObjectNode();
        Report report = new Report();
        KeyValue kv = new KeyValue();
        kv.setPayload(json.toString());
        report.setJson(kv);
        String jsonSrc = JsonUtils.serialize(report);
        System.out.println(jsonSrc);
        Report deserialized = JsonUtils.deserialize(jsonSrc, Report.class);
        System.out.println(deserialized.toString());
    }

}
