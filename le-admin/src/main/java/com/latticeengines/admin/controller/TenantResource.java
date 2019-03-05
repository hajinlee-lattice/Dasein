package com.latticeengines.admin.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.dynamicopts.DynamicOptionsService;
import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.proxy.exposed.component.ComponentProxy;
import com.latticeengines.security.exposed.Constants;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "tenantadmin", description = "REST resource for managing Lattice tenants across all products")
@RestController
@RequestMapping(value = "/tenants")
@PostAuthorize("hasRole('Platform Operations') or hasRole('DeveloperSupport') or hasRole('TENANT_CONSOLE')")
public class TenantResource {

    @Inject
    private TenantService tenantService;

    @Inject
    private FeatureFlagService featureFlagService;

    @Inject
    private DynamicOptionsService dynamicOptionsService;

    @Inject
    private ComponentProxy componentProxy;

    @PostMapping("/{tenantId}")
    @ResponseBody
    @ApiOperation(value = "Create a Lattice tenant")
    public boolean createTenant(@PathVariable String tenantId, //
            @RequestParam(value = "contractId") String contractId, //
            @RequestBody TenantRegistration registration, HttpServletRequest request) {
        String ticket = request.getHeader(Constants.AUTHORIZATION);
        String userName = "_defaultUser";
        if (!StringUtils.isEmpty(ticket)) {
            String decrypted = CipherUtils.decrypt(ticket);
            String[] tokens = decrypted.split("\\|");
            userName = tokens[0];
        }
        return tenantService.createTenant(contractId.trim(), tenantId.trim(), registration, userName);
    }

    @PostMapping("/{tenantId}/V2")
    @ResponseBody
    @ApiOperation(value = "Create a Lattice tenant")
    public boolean createTenantV2(@PathVariable String tenantId, //
                                @RequestParam(value = "contractId") String contractId, //
                                @RequestBody TenantRegistration registration, HttpServletRequest request) {
        String ticket = request.getHeader(Constants.AUTHORIZATION);
        String userName = "_defaultUser";
        if (!StringUtils.isEmpty(ticket)) {
            String decrypted = CipherUtils.decrypt(ticket);
            String[] tokens = decrypted.split("\\|");
            userName = tokens[0];
        }
        return tenantService.createTenantV2(contractId.trim(), tenantId.trim(), registration, userName);
    }

    @PutMapping("/{tenantId}/services/{serviceName}")
    @ResponseBody
    @ApiOperation(value = "Bootstrap a Lattice tenant service")
    public boolean bootstrapTenant(@PathVariable String tenantId, //
            @PathVariable String serviceName, @RequestParam(value = "contractId") String contractId, //
            @RequestBody Map<String, String> overrideProperties) {
        return tenantService.bootstrap(contractId, tenantId, serviceName, overrideProperties);
    }

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get tenants for a particular contract id, or all tenants if contractId is null")
    public List<TenantDocument> getTenants(@RequestParam(value = "contractId", required = false) String contractId) {
        String parsedContractId;
        if (StringUtils.isEmpty(contractId)) {
            parsedContractId = null;
        } else {
            parsedContractId = contractId;
        }
        return new ArrayList<>(tenantService.getTenantsInCache(parsedContractId));
    }

    @GetMapping("/defaultspaceconfig")
    @ResponseBody
    @ApiOperation(value = "Get default space configuration")
    public SpaceConfiguration getDefaultSpaceConfig() {
        return tenantService.getDefaultSpaceConfig();
    }

    @DeleteMapping("/{tenantId}")
    @ResponseBody
    @ApiOperation(value = "Delete tenant for a particular contract id")
    public boolean deleteTenant(@RequestParam(value = "contractId") String contractId,
            @RequestParam(value = "deleteZookeeper", required = false, defaultValue = "true") Boolean deleteZookeeper,
            @PathVariable String tenantId, HttpServletRequest request) {
        String ticket = request.getHeader(Constants.AUTHORIZATION);
        String userName = "_defaultUser";
        if (!StringUtils.isEmpty(ticket)) {
            String decrypted = CipherUtils.decrypt(ticket);
            String[] tokens = decrypted.split("\\|");
            userName = tokens[0];
        }
        return tenantService.deleteTenant(userName, contractId, tenantId, deleteZookeeper);
    }

    @GetMapping("/{tenantId}")
    @ResponseBody
    @ApiOperation(value = "Get tenant for a particular contract id")
    public TenantDocument getTenant(@RequestParam(value = "contractId") String contractId,
            @PathVariable String tenantId) {
        contractId = contractId.replace("?", "");
        return tenantService.getTenant(contractId, tenantId);
    }

    @GetMapping("/{tenantId}/services/{serviceName}")
    @ResponseBody
    @ApiOperation(value = "Get config for currently provisioned tenant service")
    public SerializableDocumentDirectory getServiceConfig(@RequestParam(value = "contractId") String contractId, //
            @PathVariable String tenantId, @PathVariable String serviceName) {
        SerializableDocumentDirectory config = tenantService.getTenantServiceConfig(contractId, tenantId, serviceName);
        config.setRootPath("/" + serviceName);
        config = dynamicOptionsService.bind(config);
        return config;
    }

    @GetMapping("/{tenantId}/services/{serviceName}/state")
    @ResponseBody
    @ApiOperation(value = "Get state for tenant service")
    public BootstrapState getServiceState(@RequestParam(value = "contractId") String contractId, //
            @PathVariable String tenantId, @PathVariable String serviceName) {
        return tenantService.getTenantServiceState(contractId, tenantId, serviceName);
    }

    @GetMapping("/topologies")
    @ResponseBody
    @ApiOperation(value = "Get list of available topologies")
    public List<String> getTopologies() {
        List<String> topologies = new ArrayList<>();
        for (CRMTopology topology : CRMTopology.values()) {
            topologies.add(topology.getName());
        }
        return topologies;
    }

    @GetMapping("/products")
    @ResponseBody
    @ApiOperation(value = "Get list of available products")
    public List<String> getProducts() {
        List<String> products = new ArrayList<>();
        for (LatticeProduct product : LatticeProduct.values()) {
            products.add(product.getName());
        }
        return products;
    }

    @GetMapping("/{tenantId}/featureflags")
    @ResponseBody
    @ApiOperation(value = "Get feature flags for a tenant")
    public FeatureFlagValueMap getFlags(@PathVariable String tenantId) {
        return featureFlagService.getFlags(tenantId);
    }

    @PostMapping("/{tenantId}/featureflags")
    @ResponseBody
    @ApiOperation(value = "Set feature flags for a tenant")
    public FeatureFlagValueMap setFlag(@PathVariable String tenantId, @RequestBody FeatureFlagValueMap flags) {
        for (Map.Entry<String, Boolean> flag : flags.entrySet()) {
            featureFlagService.setFlag(tenantId, flag.getKey(), flag.getValue());
        }
        return featureFlagService.getFlags(tenantId);
    }

    @DeleteMapping("/{tenantId}/featureflags/{flagId}")
    @ResponseBody
    @ApiOperation(value = "Remove a feature flag from a tenant")
    public FeatureFlagValueMap removeFlag(@PathVariable String tenantId, @PathVariable String flagId) {
        featureFlagService.removeFlag(tenantId, flagId);
        return featureFlagService.getFlags(tenantId);
    }

    @PutMapping("/{contractId}/{tenantId}")
    @ResponseBody
    @ApiOperation(value = "Bootstrap a Lattice tenant service")
    public boolean updateTenantInfo(@PathVariable(value = "contractId") String contractId, //
            @PathVariable String tenantId, //
            @RequestBody TenantInfo tenantInfo) {
        return tenantService.updateTenantInfo(contractId, tenantId, tenantInfo);
    }

    @PostMapping("/{tenantId}/reset")
    @ResponseBody
    @ApiOperation(value = "Reset tenant")
    public boolean resetTenant(@PathVariable String tenantId) {
        boolean result_cdl = componentProxy.reset(tenantId, ComponentConstants.CDL);
        boolean result_lp = componentProxy.reset(tenantId, ComponentConstants.LP);
        boolean result_metadata = componentProxy.reset(tenantId, ComponentConstants.METADATA);

        return result_cdl && result_lp && result_metadata;
    }
}
