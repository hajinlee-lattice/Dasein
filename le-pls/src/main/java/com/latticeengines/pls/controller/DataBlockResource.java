package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.UIAction;
import com.latticeengines.domain.exposed.exception.UIActionCode;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.exception.UIActionUtils;
import com.latticeengines.domain.exposed.exception.View;
import com.latticeengines.proxy.exposed.dcp.EntitlementProxy;
import com.latticeengines.proxy.exposed.matchapi.PrimeMetadataProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Data Block")
@RestController
@RequestMapping("/data-blocks")
public class DataBlockResource {

    private static final Logger log = LoggerFactory.getLogger(DataBlockResource.class);

    @Inject
    private PrimeMetadataProxy primeMetadataProxy;

    @Inject
    private EntitlementProxy entitlementProxy;

    @Deprecated
    @GetMapping("/metadata")
    @ResponseBody
    @ApiOperation(value = "Get block metadata")
    public DataBlockMetadataContainer getBlockMetadata() {
        return primeMetadataProxy.getBlockMetadata();
    }

    @Deprecated
    @GetMapping("/elements")
    @ResponseBody
    @ApiOperation(value = "Get block-level-element tree")
    public List<DataBlock> getBlocks(@RequestParam(value = "blockIds", required = false) List<String> blockIds) {
        return primeMetadataProxy.getBlockElements(blockIds);
    }

    private String encodeDataDomain(String domainName) throws UIActionException {
        log.info("Attempting to encode domain name " + domainName);
        if ("ALL".equals(domainName)) {
            return domainName;
        } else {
            try {
                DataDomain domain = DataDomain.parse(domainName);
                String dataDomainName = domain.name();
                log.info("Encoded domain name " + domainName + " as " + dataDomainName);
                return dataDomainName;
            } catch (Exception e) {
                log.error("Failed to parse domain name " + domainName, e);
                String title = "Invalid Domain Name.";
                UIActionCode uiActionCode = UIActionCode.fromLedpCode(LedpCode.LEDP_00002);
                UIAction action = UIActionUtils.generateUIError(title, View.Banner, uiActionCode);
                throw UIActionException.fromAction(action);
            }
        }
    }

    private String encodeRecordType(String recordType) throws UIActionException {
        log.info("Attempting to encode record type " + recordType);
        if ("ALL".equals(recordType)) {
            return recordType;
        } else {
            try {
                DataRecordType dataRecordType = DataRecordType.parse(recordType);
                String recordTypeName = dataRecordType.name();
                log.info("Encoded record type " + recordType + " as " + recordTypeName);
                return recordTypeName;
            } catch (Exception e) {
                log.error("Failed to parse record type " + recordType, e);
                String title = "Invalid Record Type.";
                UIActionCode uiActionCode = UIActionCode.fromLedpCode(LedpCode.LEDP_00002);
                UIAction action = UIActionUtils.generateUIError(title, View.Banner, uiActionCode);
                throw UIActionException.fromAction(action);
            }
        }
    }

    @GetMapping("/entitlement")
    @ResponseBody
    @ApiOperation(value = "Get block drt entitlement")
    @PreAuthorize("hasRole('Edit_DCP_Projects')")
    public DataBlockEntitlementContainer getEntitlement(
            @RequestParam(value = "domainName", required = false, defaultValue = "ALL") String domainName,
            @RequestParam(value = "recordType", required = false, defaultValue = "ALL") String recordType,
            @RequestParam(value = "includeElements", required = false, defaultValue = "false") Boolean includeElements) {
        try {
            String encodedDomainName = encodeDataDomain(domainName);
            String encodedRecordType = encodeRecordType(recordType);

            DataBlockEntitlementContainer dataBlockEntitlementContainer = entitlementProxy
                    .getEntitlement(MultiTenantContext.getShortTenantId(), encodedDomainName, encodedRecordType);

            if ((!"ALL".equals(domainName) || !"ALL".equals(recordType))
                    && dataBlockEntitlementContainer.getDomains().isEmpty()) {
                String title = "Subscriber not entitled to the given domain and record type combination.";
                UIActionCode uiActionCode = UIActionCode.fromLedpCode(LedpCode.LEDP_00002);
                UIAction action = UIActionUtils.generateUIError(title, View.Banner, uiActionCode);
                throw UIActionException.fromAction(action);
            } else {
                if (includeElements) {
                    return primeMetadataProxy.enrichEntitlementContainerWithElements(dataBlockEntitlementContainer);
                }
                return dataBlockEntitlementContainer;
            }
        } catch (Exception ex) {
            throw UIActionUtils.handleException(ex);
        }
    }
}
