package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.service.CrmCredentialService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "CRM Credential Verification", description = "REST resource for CRM credential verification")
@RestController
@RequestMapping(value = "/credentials")
@PreAuthorize("hasRole('Edit_PLS_Configuration')")
public class CrmCredentialResource {

    @Autowired
    private CrmCredentialService crmCredentialService;

    @RequestMapping(value = "/{crmType}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Verify CRM credential")
    public CrmCredential verifyCredential(@PathVariable String crmType, @RequestParam(value = "tenantId") String tenantId,
            @RequestParam(value = "contractId") String contractId, @RequestBody CrmCredential crmCredential) {

        CrmCredential newCrmCredential = crmCredentialService.verifyCredential(crmType, tenantId, contractId, crmCredential);
        return newCrmCredential;

    }

    @RequestMapping(value = "/{crmType}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get CRM credential")
    public CrmCredential getCredential(@PathVariable String crmType, @RequestParam(value = "tenantId") String tenantId,
            @RequestParam(value = "contractId") String contractId) {

        return crmCredentialService.getCredential(crmType, tenantId, contractId);

    }
}
