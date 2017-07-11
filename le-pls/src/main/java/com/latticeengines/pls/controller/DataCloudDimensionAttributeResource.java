package com.latticeengines.pls.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.proxy.exposed.matchapi.DimensionAttributeProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dimensionattributes", description = "REST resource for Dimension Attributes")
@RestController
@RequestMapping("/dimensionattributes")
public class DataCloudDimensionAttributeResource {

    @Autowired
    private DimensionAttributeProxy dimensionAttributeProxy;

    @RequestMapping(value = "/dimensions", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all top level Dimentions")
    public List<CategoricalDimension> getAllDimensions() {
        List<CategoricalDimension> allDimensions = dimensionAttributeProxy.getAllDimensions();
        return allDimensions;
    }

    @RequestMapping(value = "/attributes", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all attributes by Dimension's root Id")
    public List<CategoricalAttribute> getAllAttributes(
            @RequestParam(value = "rootId", required = true) Long rootId) {
        List<CategoricalAttribute> allAttributes = dimensionAttributeProxy.getAllAttributes(rootId);
        return allAttributes;
    }
}
