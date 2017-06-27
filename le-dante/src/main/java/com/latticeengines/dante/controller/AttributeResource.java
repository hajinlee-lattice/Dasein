package com.latticeengines.dante.controller;

import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dante.service.AttributeService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.network.exposed.dante.DanteAttributesInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante", description = "REST resource for attributes related to Dante notions")
@RestController
@RequestMapping("/attributes")
public class AttributeResource implements DanteAttributesInterface {
    private static final Logger log = Logger.getLogger(AttributeResource.class);

    @Autowired
    private AttributeService attributeService;

    @RequestMapping(value = "/accountattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get account attributes for this tenant")
    public ResponseDocument<Map<String, String>> getAccountAttributes(
            @RequestParam("customerSpace") String customerSpace) {
        return ResponseDocument.successResponse(attributeService.getAccountAttributes(customerSpace));
    }

    @RequestMapping(value = "/recommendationattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get recommendation attributes")
    public ResponseDocument<Map<String, String>> getRecommendationAttributes(
            @RequestParam("customerSpace") String customerSpace) {
        return ResponseDocument.successResponse(attributeService.getRecommendationAttributes(customerSpace));
    }
}
