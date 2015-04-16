package com.latticeengines.admin.controller;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
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
    @ApiOperation(value = "Create a Lattice tenant")
    public Boolean createTenant(@PathVariable String tenantId, //
            @RequestParam(value = "contractId") String contractId, //
            @RequestBody CustomerSpaceInfo info) {
        return tenantService.createTenant(contractId, tenantId, info);
    }

    @RequestMapping(value = "/{tenantId}/services/{serviceName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Bootstrap a Lattice tenant service")
    public Boolean bootstrapTenant(@PathVariable String tenantId, //
            @PathVariable String serviceName, @RequestParam(value = "contractId") String contractId, //
            @RequestBody Map<String, String> overrideProperties) {
        return tenantService.bootstrap(contractId, tenantId, serviceName, overrideProperties);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenants for a particular contract id, or all tenants if contractId is null")
    public List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(
            @RequestParam(value = "contractId", required = false) String contractId) {
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

    @RequestMapping(value = "/{tenantId}/services/{serviceName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get config for currently provisioned tenant service")
    public SerializableDocumentDirectory getServiceConfig(@RequestParam(value = "contractId") String contractId, //
            @PathVariable String tenantId, @PathVariable String serviceName) {
        return tenantService.getTenantServiceConfig(contractId, tenantId, serviceName);
    }

    @RequestMapping(value = "/services/", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of tenant services")
    public List<String> getServices() {
        return new ArrayList<String>(tenantService.getRegisteredServiceKeySet());
    }

    @RequestMapping(value = "/services/{serviceName}/metadata", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get metadata of particular metadata service")
    public String getServiceMetadata(@PathVariable String serviceName) {
        return tenantService.getTenantServiceMetadata(serviceName);
    }

    @RequestMapping(value = "/services/{serviceName}/default", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get default config for tenant service")
    public SerializableDocumentDirectory getServiceDefaultConfig(@PathVariable String serviceName) {
        return tenantService.getDefaultTenantServiceConfig(serviceName);
    }

    @RequestMapping(value = "/{tenantId}/services/{serviceName}/state", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get state for tenant service")
    public BootstrapState getServiceState(@RequestParam(value = "contractId") String contractId, //
            @PathVariable String tenantId, @PathVariable String serviceName) {
        return tenantService.getTenantServiceState(contractId, tenantId, serviceName);
    }
}
