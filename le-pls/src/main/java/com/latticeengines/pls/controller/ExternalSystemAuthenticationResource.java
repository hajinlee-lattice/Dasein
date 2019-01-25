package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.proxy.exposed.cdl.ExternalSystemAuthenticationProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "external-system-authentication", description = "Rest resource for External System Authentications")
@RestController
@RequestMapping("/external-system-authentication")
public class ExternalSystemAuthenticationResource {

    @Inject
    private ExternalSystemAuthenticationProxy extSysAuthenticationProxy;

    @PostMapping(value = "/")
    @ResponseBody
    @ApiOperation(value = "create external authentication")
    public ExternalSystemAuthentication createAuthentication(@RequestBody ExternalSystemAuthentication externalSystemAuthentication) {
        return extSysAuthenticationProxy.createAuthentication(MultiTenantContext.getTenant().getId(), externalSystemAuthentication);
    }

    @PutMapping(value = "/{authId}")
    @ResponseBody
    @ApiOperation(value = "update external authentication")
    public ExternalSystemAuthentication updateAuthentication(@PathVariable String authId, @RequestBody ExternalSystemAuthentication externalSystemAuthentication) {
        return extSysAuthenticationProxy.updateAuthentication(MultiTenantContext.getTenant().getId(), authId, externalSystemAuthentication);
    }

    @GetMapping(value = "/{authId}")
    @ResponseBody
    @ApiOperation(value = "Get external authentication by Id")
    public ExternalSystemAuthentication findAuthenticationByAuthId(@PathVariable String authId) {
        return extSysAuthenticationProxy.findAuthenticationByAuthId(MultiTenantContext.getTenant().getId(), authId);
    }

    @GetMapping(value = "/")
    @ResponseBody
    @ApiOperation(value = "Get All external authentications")
    public List<ExternalSystemAuthentication> findAuthentications() {
        return extSysAuthenticationProxy.findAuthentications(MultiTenantContext.getTenant().getId());
    }

    @GetMapping(value = "/lookupid-mappings")
    @ResponseBody
    @ApiOperation(value = "Get external authentication by list of LookupIdMappings")
    public List<ExternalSystemAuthentication> findAuthenticationsByLookupIdMappings(
            @RequestParam(value = "mapping_ids", required = true) List<String> mappingIds) {
        return extSysAuthenticationProxy.findAuthenticationsByLookupMapIds(MultiTenantContext.getTenant().getId(), mappingIds);
    }
}
