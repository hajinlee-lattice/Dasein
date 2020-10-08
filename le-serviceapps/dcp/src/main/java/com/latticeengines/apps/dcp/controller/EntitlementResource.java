package com.latticeengines.apps.dcp.controller;

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
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Entitlement Management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/entitlement")
public class EntitlementResource {

    private static final Logger log = LoggerFactory.getLogger(EntitlementResource.class);

    @Inject
    private EntitlementService entitlementService;

    private String decodeDomainName(String domainNameParameter) {
        log.info("Attempting to decode domain name " + domainNameParameter);
        if ("ALL".equals(domainNameParameter)) {
            return domainNameParameter;
        } else {
            String parsedDomainName = DataDomain.parse(domainNameParameter).getDisplayName();
            log.info("Parsed domain name as " + parsedDomainName);
            return parsedDomainName;
        }
    }

    private String decodeRecordType(String recordTypeParameter) {
        log.info("Attempting to record type " + recordTypeParameter);
        if ("ALL".equals(recordTypeParameter)) {
            return recordTypeParameter;
        } else {
            String parsedRecordType = DataRecordType.parse(recordTypeParameter).getDisplayName();
            log.info("Parsed record type as " + parsedRecordType);
            return parsedRecordType;
        }
    }

    @GetMapping("/{domainName}/{recordType}")
    @ResponseBody
    @ApiOperation(value = "Get block drt entitlement")
    public DataBlockEntitlementContainer getEntitlement(@PathVariable String customerSpace,
            @PathVariable String domainName, @PathVariable String recordType) {
        return entitlementService.getEntitlement(customerSpace, decodeDomainName(domainName),
                decodeRecordType(recordType));
    }

}
