package com.latticeengines.apps.dcp.controller;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.EntitlementService;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Entitlement Management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/entitlement")
public class EntitlementResource {

    private static final Logger log = LoggerFactory.getLogger(EntitlementResource.class);

    @Inject
    private EntitlementService entitlementService;

    private String decodeURLParameter(String parameter) {
        log.info("Attempting to decode URL parameter " + parameter);
        try {
            return URLDecoder.decode(parameter, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            log.error("Unexpected error decoding URL parameter " + parameter, e);
            return "ALL";
        }
    }

    @GetMapping("/{domainName}/{recordType}")
    @ResponseBody
    @ApiOperation(value = "Get block drt entitlement")
    public DataBlockEntitlementContainer getEntitlement(@PathVariable String customerSpace,
            @PathVariable String domainName, @PathVariable String recordType) {
        return entitlementService.getEntitlement(customerSpace, decodeURLParameter(domainName),
                decodeURLParameter(recordType));
    }

}
