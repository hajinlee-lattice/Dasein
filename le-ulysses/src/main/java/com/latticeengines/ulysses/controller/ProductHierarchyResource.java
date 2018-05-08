package com.latticeengines.ulysses.controller;

import java.util.List;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ProductHierarchy", description = "Common REST resource for product hierarchy data")
@RestController
@RequestMapping("/productHierarchy")
public class ProductHierarchyResource {

    @RequestMapping(value = "/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the purchase history data for the given account")
    public FrontEndResponse<List<String>> getProductHierarchy() {
        throw new UnsupportedOperationException();
    }
}
