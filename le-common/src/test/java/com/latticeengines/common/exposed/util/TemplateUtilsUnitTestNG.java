package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;


public class TemplateUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testRenderJson() throws IOException {
        JsonNode json = new ObjectMapper() //
                .readTree("{\"k1\": \"val 1\", \"k2\": [1, 2], \"k3\" : {\"k31\": \"val 3\"}}");

        String template = "par1=${k1.asText()}, par2=${k2.get(1).asInt()?string.percent}, par3=${k3.k31.asText()}";
        String rendered = TemplateUtils.renderByJson(template, json);
        System.out.println(rendered);
        Assert.assertEquals(rendered, "par1=val 1, par2=200%, par3=val 3");

        template = "${k2.iterator()?join(\", \")}";
        rendered = TemplateUtils.renderByJson(template, json);
        System.out.println(rendered);
        Assert.assertEquals(rendered, "1, 2");
    }

    @Test(groups = "unit")
    public void testRenderMap() {
        Map<String, Object> params=  new HashMap<>();
        params.put("k1", "val 1");
        params.put("k2", Arrays.asList(1, 2));
        params.put("k3", ImmutableMap.of("k31", "val 3"));

        String template = "par1=${k1}, par2=${k2[1]?string.percent}, par3=${k3.k31}";
        String rendered = TemplateUtils.renderByMap(template, params);
        System.out.println(rendered);
        Assert.assertEquals(rendered, "par1=val 1, par2=200%, par3=val 3");

        template = "${k2?join(\", \")}";
        rendered = TemplateUtils.renderByMap(template, params);
        System.out.println(rendered);
        Assert.assertEquals(rendered, "1, 2");
    }

    @Test(groups = "unit")
    public void testIfThen() {
        Map<String, Object> params=  new HashMap<>();
        params.put("val", "val 1");
        String template = "Value is <#if val == '__others__'>others<#else>${val}</#if>";
        String rendered = TemplateUtils.renderByMap(template, params);
        Assert.assertEquals(rendered, "Value is val 1");

        params.put("val", "__others__");
        rendered = TemplateUtils.renderByMap(template, params);
        Assert.assertEquals(rendered, "Value is others");
    }

}
