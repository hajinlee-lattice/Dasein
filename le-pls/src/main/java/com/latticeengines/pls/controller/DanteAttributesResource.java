package com.latticeengines.pls.controller;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.dante.DanteAttributesProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante/attributes", description = "REST resource for attributes related to Dante notions")
@RestController
@RequestMapping("/dante/attributes")
@PreAuthorize("hasRole('View_PLS_Plays')")
public class DanteAttributesResource {

    private static final Logger log = LoggerFactory.getLogger(DanteAttributesResource.class);

    @Autowired
    DanteAttributesProxy danteAttributesProxy;

    @RequestMapping(value = "/accountattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get account attributes for this tenant")
    public ResponseDocument<Map<String, String>> getAccountAttributes() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return danteAttributesProxy.getAccountAttributes(customerSpace.toString());
    }

    @RequestMapping(value = "/recommendationattributes", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get recommendation attributes")
    public ResponseDocument<Map<String, String>> getRecommendationAttributes() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return danteAttributesProxy.getRecommendationAttributes(customerSpace.toString());
    }
}
