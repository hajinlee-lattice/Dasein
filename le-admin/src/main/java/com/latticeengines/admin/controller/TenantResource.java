package com.latticeengines.admin.controller;

import java.util.AbstractMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;


@Api(value = "tenantadmin", description = "REST resource for managing Lattice tenants across all products")
@RestController
@RequestMapping(value = "/tenants")
public class TenantResource {

    @Autowired
    private TenantService tenantService;
    
    @RequestMapping(value = "/{tenantId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Bootstrap a Lattice tenant")
    public void bootstrap(@RequestParam(value = "contractId") String contractId, @PathVariable String tenantId) {
       
    }
    
    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenants for a particular contract id, or all tenants if contractId is null")
    public List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(@RequestParam(value = "contractId", required = false) String contractId) {
        if (StringUtils.isEmpty(contractId)) {
            contractId = null;
        }
        return tenantService.getTenants(contractId);
    }
    
    @RequestMapping(value = "/{tenantId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete tenant for a particular contract id")
    public Boolean deleteTenant(@RequestParam(value = "contractId") String contractId, @PathVariable String tenantId) {
        return tenantService.deleteTenant(contractId, tenantId);
    }
    
    @RequestMapping(value = "/{tenantId}/services/{serviceId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get config for currently provisioned tenant service")
    public DocumentDirectory getServiceConfig(@RequestParam(value = "contractId") String contractId, //
            @PathVariable String tenantId, @PathVariable String serviceId) {
        return null;
    }
    
    @RequestMapping(value = "/{tenantId}/services/{serviceId}/state", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get state for tenant service")
    public JsonNode getServiceState(@RequestParam(value = "contractId") String contractId, //
            @PathVariable String tenantId, @PathVariable String serviceId) {
        return null;
    }
}
