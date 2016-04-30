package com.latticeengines.admin.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.dynamicopts.DynamicOptionsService;
import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "tenantadmin", description = "REST resource for managing Lattice tenants across all products")
@RestController
@RequestMapping(value = "/tenants")
@PostAuthorize("hasRole('Platform Operations') or hasRole('DeveloperSupport') or hasRole('QA')")
public class TenantResource {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private FeatureFlagService featureFlagService;

    @Autowired
    private DynamicOptionsService dynamicOptionsService;

    @RequestMapping(value = "/{tenantId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a Lattice tenant")
    public boolean createTenant(@PathVariable String tenantId, //
            @RequestParam(value = "contractId") String contractId, //
            @RequestBody TenantRegistration registration) {
        return tenantService.createTenant(contractId, tenantId, registration);
    }

    @RequestMapping(value = "/{tenantId}/services/{serviceName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Bootstrap a Lattice tenant service")
    public boolean bootstrapTenant(@PathVariable String tenantId, //
            @PathVariable String serviceName, @RequestParam(value = "contractId") String contractId, //
            @RequestBody Map<String, String> overrideProperties) {
        return tenantService.bootstrap(contractId, tenantId, serviceName, overrideProperties);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenants for a particular contract id, or all tenants if contractId is null")
    public List<TenantDocument> getTenants(@RequestParam(value = "contractId", required = false) String contractId) {
        String parsedContractId;
        if (StringUtils.isEmpty(contractId)) {
            parsedContractId = null;
        } else {
            parsedContractId = contractId;
        }
        return new ArrayList<>(tenantService.getTenants(parsedContractId));
    }

    @RequestMapping(value = "/defaultspaceconfig", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get default space configuration")
    public SpaceConfiguration getDefaultSpaceConfig() {
        return tenantService.getDefaultSpaceConfig();
    }

    @RequestMapping(value = "/{tenantId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete tenant for a particular contract id")
    public boolean deleteTenant(@RequestParam(value = "contractId") String contractId, @PathVariable String tenantId) {
        return tenantService.deleteTenant(contractId, tenantId);
    }

    @RequestMapping(value = "/{tenantId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenant for a particular contract id")
    public TenantDocument getTenant(@RequestParam(value = "contractId") String contractId,
            @PathVariable String tenantId) {
        contractId = contractId.replace("?", "");
        return tenantService.getTenant(contractId, tenantId);
    }

    @RequestMapping(value = "/{tenantId}/services/{serviceName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get config for currently provisioned tenant service")
    public SerializableDocumentDirectory getServiceConfig(@RequestParam(value = "contractId") String contractId, //
            @PathVariable String tenantId, @PathVariable String serviceName) {
        SerializableDocumentDirectory config = tenantService.getTenantServiceConfig(contractId, tenantId, serviceName);
        config.setRootPath("/" + serviceName);
        config = dynamicOptionsService.bind(config);
        return config;
    }

    @RequestMapping(value = "/{tenantId}/services/{serviceName}/state", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get state for tenant service")
    public BootstrapState getServiceState(@RequestParam(value = "contractId") String contractId, //
            @PathVariable String tenantId, @PathVariable String serviceName) {
        return tenantService.getTenantServiceState(contractId, tenantId, serviceName);
    }

    @RequestMapping(value = "/topologies", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of available topologies")
    public List<String> getTopologies() {
        List<String> topologies = new ArrayList<>();
        for (CRMTopology topology : CRMTopology.values()) {
            topologies.add(topology.getName());
        }
        return topologies;
    }

    @RequestMapping(value = "/products", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of available products")
    public List<String> getProducts() {
        List<String> products = new ArrayList<>();
        for (LatticeProduct product : LatticeProduct.values()) {
            products.add(product.getName());
        }
        return products;
    }

    @RequestMapping(value = "/{tenantId}/featureflags", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get feature flags for a tenant")
    public FeatureFlagValueMap getFlags(@PathVariable String tenantId) {
        return featureFlagService.getFlags(tenantId);
    }

    @RequestMapping(value = "/{tenantId}/featureflags", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Set feature flags for a tenant")
    public FeatureFlagValueMap setFlag(@PathVariable String tenantId, @RequestBody FeatureFlagValueMap flags) {
        for (Map.Entry<String, Boolean> flag : flags.entrySet()) {
            featureFlagService.setFlag(tenantId, flag.getKey(), flag.getValue());
        }
        return featureFlagService.getFlags(tenantId);
    }

    @RequestMapping(value = "/{tenantId}/featureflags/{flagId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Remove a feature flag from a tenant")
    public FeatureFlagValueMap removeFlag(@PathVariable String tenantId, @PathVariable String flagId) {
        featureFlagService.removeFlag(tenantId, flagId);
        return featureFlagService.getFlags(tenantId);
    }
}
