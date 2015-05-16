package com.latticeengines.admin.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.service.impl.DynamicOptions;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "tenantadmin", description = "REST resource for managing Lattice tenants across all products")
@RestController
@RequestMapping(value = "/tenants")
@PreAuthorize("hasRole('Platform Operations')")
public class TenantResource {

    private static final String podId = CamilleEnvironment.getPodId();

    @Autowired
    private TenantService tenantService;

    @Autowired
    private DynamicOptions dynamicOptions;

    @RequestMapping(value = "/{tenantId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a Lattice tenant")
    public boolean createTenant(@PathVariable String tenantId, //
            @RequestParam(value = "contractId") String contractId, //
            @RequestBody TenantRegistration registration) {
        return tenantService.createTenant(contractId, tenantId, registration);
    }

    @RequestMapping(value = "/{tenantId}/services/{serviceName}",
            method = RequestMethod.PUT, headers = "Accept=application/json")
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
    public List<TenantDocument> getTenants(
            @RequestParam(value = "contractId", required = false) String contractId) {
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
    @ApiOperation(value = "Delete tenant for a particular contract id")
    public TenantDocument getTenant(@RequestParam(value = "contractId") String contractId,
                                    @PathVariable String tenantId) {
        return tenantService.getTenant(contractId, tenantId);
    }


    @RequestMapping(value = "/{tenantId}/services/{serviceName}",
            method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get config for currently provisioned tenant service")
    public SerializableDocumentDirectory getServiceConfig(@RequestParam(value = "contractId") String contractId, //
            @PathVariable String tenantId, @PathVariable String serviceName) {
        SerializableDocumentDirectory config = tenantService.getTenantServiceConfig(contractId, tenantId, serviceName);
        Path schemaPath = PathBuilder.buildServiceConfigSchemaPath(podId, serviceName);
        config.setRootPath(schemaPath.toString());
        return dynamicOptions.applyDynamicBinding(config);
    }

    @RequestMapping(value = "/{tenantId}/services/{serviceName}/state",
            method = RequestMethod.GET, headers = "Accept=application/json")
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
        for (CRMTopology topology: CRMTopology.values()) {
            topologies.add(topology.getName());
        }
        return topologies;
    }

    @RequestMapping(value = "/products", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of available products")
    public List<String> getProducts() {
        List<String> products = new ArrayList<>();
        for (LatticeProduct product: LatticeProduct.values()) {
            products.add(product.getName());
        }
        return products;
    }
}
