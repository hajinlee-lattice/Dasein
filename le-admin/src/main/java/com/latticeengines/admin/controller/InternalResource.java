package com.latticeengines.admin.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.Components.ComponentsMap;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.UserService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "internal_service_resource", description = "REST service resource for internal operations")
@RestController
@RequestMapping("/internal")
public class InternalResource {

    @Inject
    private TenantService tenantService;

    @Inject
    private ServiceService serviceService;

    @Inject
    private DynamicOptionsService dynamicOptionsService;

    @Inject
    private UserService userService;

    @Inject
    private BatonService batonService;

    @GetMapping("services/options")
    @ResponseBody
    @ApiOperation(value = "Get all configuration fields that are the type of option")
    public SelectableConfigurationDocument getServiceOptionalConfigs(
            @RequestParam(value = "component") String component) {
        final SelectableConfigurationDocument doc = serviceService.getSelectableConfigurationFields(component, false);
        if (doc == null) {
            throw new LedpException(LedpCode.LEDP_19102, new String[] { component });
        }
        return dynamicOptionsService.bind(doc);
    }

    @PutMapping("services/options")
    @ResponseBody
    @ApiOperation(value = "Update dropdown options of a field")
    public Boolean patchServiceOptionalConfigs(@RequestParam(value = "component") String component,
            @RequestBody SelectableConfigurationField patch) {
        if (patch.getDefaultOption() != null) {
            return serviceService.patchDefaultConfigWithOptions(component, patch);
        } else {
            if (existingDefaultIsValid(component, patch)) {
                return serviceService.patchOptions(component, patch);
            } else {
                throw new LedpException(LedpCode.LEDP_19105,
                        new String[] { patch.getOptions().toString(), patch.getDefaultOption() });
            }
        }
    }

    @GetMapping("/tenants")
    @ResponseBody
    @ApiOperation(value = "Get all tenants ids")
    public List<String> getTenants() {
        Collection<TenantDocument> tenants = tenantService.getTenantsInCache(null);
        if (CollectionUtils.isNotEmpty(tenants)) {
            return tenants.stream().map(doc -> doc.getSpace().toString()).collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }
    }

    @DeleteMapping("tenants/{tenantId}")
    @ResponseBody
    @ApiOperation(value = "Delete tenant for a particular contract id")
    public boolean deleteTenant(@RequestParam(value = "contractId") String contractId, @PathVariable String tenantId,
            HttpServletRequest request) {
        String userName = getUsernameFromHeader(request);
        return tenantService.deleteTenant(userName, contractId, tenantId, true);
    }

    @GetMapping("datastore/{option}/{tenantId}")
    @ResponseBody
    @ApiOperation(value = "Get files of a tenant in datastore")
    public List<String> getTenantFoldersInDatastore(@PathVariable String option, @PathVariable String tenantId) {
        return new ArrayList<>();
    }

    @DeleteMapping("datastore/{server}/{tenantId}")
    @ResponseBody
    @ApiOperation(value = "Delete a tenant from datastore")
    public Boolean deleteTenantInDatastore(@PathVariable String server, @PathVariable String tenantId) {
        return true;
    }

    @PutMapping("services/deactiveUserStatus")
    @ResponseBody
    @ApiOperation(value = "set user status to inactive")
    public Boolean deactiveUserStatusBasedOnEmails(@RequestBody String emails, HttpServletRequest request) {
        String userName = getUsernameFromHeader(request);
        userService.deactiveUserStatus(userName, emails);
        serviceService.reduceConfig(PLSComponent.componentName, emails);
        return true;
    }

    @GetMapping("permstore/{option}/{server}/{tenant}")
    @ResponseBody
    @ApiOperation(value = "Get file names in permstore")
    public Boolean hasVDBInPermstore(@PathVariable String option, @PathVariable String server,
            @PathVariable String tenant) {
        return Boolean.FALSE;
    }

    @DeleteMapping("permstore/{option}/{server}/{tenant}")
    @ResponseBody
    @ApiOperation(value = "Delete file in permstore")
    public Boolean deleteVDBInPermstore(@PathVariable String option, @PathVariable String server,
            @PathVariable String tenant) {
        return Boolean.TRUE;
    }

    private boolean existingDefaultIsValid(String serverName, SelectableConfigurationField patch) {
        SerializableDocumentDirectory defaultDir = serviceService.getDefaultServiceConfig(serverName);
        String defaultOption = defaultDir.getNodeAtPath(patch.getNode()).getData();
        patch.setDefaultOption(defaultOption);
        return patch.defaultIsValid();
    }

    @PutMapping("services/addUserAccessLevel")
    @ResponseBody
    @ApiOperation(value = "add user Access level")
    public Boolean addUserAccessLevel(@RequestBody String emails,
            @RequestParam(value = "right", required = false, defaultValue = "SUPER_ADMIN") String right,
            HttpServletRequest request) {
        AccessLevel level = AccessLevel.valueOf(right);
        String userName = getUsernameFromHeader(request);
        boolean success = false;
        String filterEmails = userService.addUserAccessLevel(userName, emails, level);
        if (!StringUtils.isEmpty(filterEmails)) {
            success = serviceService.patchNewConfig(PLSComponent.componentName, level, filterEmails);
        }
        return success;
    }

    private String getUsernameFromHeader(HttpServletRequest request) {
        String ticket = request.getHeader(Constants.AUTHORIZATION);
        String userName = "_defaultUser";
        if (!StringUtils.isEmpty(ticket)) {
            String decrypted = CipherUtils.decrypt(ticket);
            String[] tokens = decrypted.split("\\|");
            userName = tokens[0];
        }
        return userName;
    }

    @PostMapping("/{tenantId}/components")
    @ResponseBody
    @ApiOperation(value = "Set components for a tenant")
    public Boolean setComponents(@PathVariable String tenantId, @RequestBody ComponentsMap components) {
        boolean allowAutoSchedule = batonService.isEnabled(CustomerSpace.parse(tenantId),
                LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE);
        for (HashMap.Entry<String, HashMap<String, String>> entry : components.entrySet()) {
            String service = entry.getKey();
            HashMap<String, String> nodes = entry.getValue();
            for (HashMap.Entry<String, String> node : nodes.entrySet()) {
                 serviceService.patchTenantServiceConfig(tenantId, service, allowAutoSchedule, node.getKey(),
                         node.getValue());
            }
        }
        return true;
    }
}
