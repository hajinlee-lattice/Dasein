package com.latticeengines.pls.controller;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "sureshot", description = "REST resource for providing SureShot links")
@RestController
@RequestMapping(value = "/sureshot")
@PreAuthorize("hasRole('View_PLS_Configuration')")
public class SureShotResource {

    @RequestMapping(value = "/credentials", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Configure Credentials")
    @PreAuthorize("hasRole('Edit_PLS_Configuration')")
    public String getConfigureCredentialLink(@RequestParam(value = "crmType") String crmType,
            @RequestParam(value = "tenantId") String tenantId) {
        return " https://incindio-qa.azurewebsites.net/lattice/credentials/" + crmType + "?tenant=" + tenantId;

    }
}
