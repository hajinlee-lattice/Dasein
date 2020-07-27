package com.latticeengines.domain.exposed.exception;

import static com.latticeengines.domain.exposed.exception.LedpCode.LEDP_00009;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public class UIActionUnitTestNG {

    @Test(groups = "unit")
    public void testUIActionTemplate() {
        // LedpCode message
        UIAction uiAction = new UIAction(UIActionCode.TEST_00);
        Assert.assertEquals(uiAction.getMessage(), LEDP_00009.getMessage());

        // in-line template
        uiAction = new UIAction(UIActionCode.TEST_01, ImmutableMap.of("user", "James Clerk Maxwell"));
        Assert.assertEquals(uiAction.getMessage(), "<p>Welcome James Clerk Maxwell!</p>");

        // render by Map
        String action = "Purchase Product";
        String productUrl = "https://my-website.com/catalog/1";
        String productName = "Product 1";
        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put("action", action);
        paramsMap.put("product", ImmutableMap.of("url", productUrl, "name", productName));
        uiAction = new UIAction(UIActionCode.TEST_02, paramsMap);
        Assert.assertTrue(uiAction.getMessage().contains("<h1>Error while performing " + action + "!</h1>"));
        Assert.assertTrue(uiAction.getMessage().contains("<a href=\"" + productUrl + "\">" + productName + "</a>"));

        // render by Json
        Test3Params params = new Test3Params();
        Test3Params.Product product = new Test3Params.Product();
        product.url = productUrl;
        product.name = productName;
        params.action = action;
        params.product = product;
        uiAction = new UIAction(UIActionCode.TEST_03, params);
        Assert.assertTrue(uiAction.getMessage().contains("<h1>Error while performing " + action + "!</h1>"));
        Assert.assertTrue(uiAction.getMessage().contains("<a href=\"" + productUrl + "\">" + productName + "</a>"));
    }

    private static class Test3Params implements UIActionParams {
        @JsonProperty
        private String action;
        @JsonProperty
        private Product product;

        private static class Product {
            @JsonProperty
            private String url;
            @JsonProperty
            private String name;
        }
    }

}
