package com.latticeengines.admin.controller.dcp;

import java.util.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.web.bind.annotation.*;

import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.*;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.*;
import com.latticeengines.domain.exposed.dcp.vbo.User;
import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.security.exposed.Constants;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "vboadmin", description = "REST resource for vbo request")
@RestController
@RequestMapping(value = "/dcp/vboadmin")
@PostAuthorize("hasRole('adminconsole')")
public class VboResource {

    @Inject
    private TenantService tenantService;

    @Inject
    private ServiceService serviceService;

    @Inject
    private FeatureFlagService featureFlagService;

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Create a DCP tenant from VBO request")
    public boolean createTenant(@RequestBody VboRequest vboRequest, HttpServletRequest request) {
        String userName = getUserName(request);

        String tenantName = vboRequest.getSubscriber().getName();

        // TenantInfo
        TenantProperties tenantProperties = new TenantProperties();
        tenantProperties.description = "A tenant created by vbo request";
        tenantProperties.displayName = tenantName;
        TenantInfo tenantInfo = new TenantInfo(tenantProperties);

        // FeatureFlags
        FeatureFlagDefinitionMap definitionMap = featureFlagService.getDefinitions();
        FeatureFlagValueMap defaultValueMap = new FeatureFlagValueMap();
        definitionMap.forEach((flagId, flagDef) -> {
            boolean defaultVal = flagDef.getDefaultValue();
            defaultValueMap.put(flagId, defaultVal);
        });

        // SpaceInfo
        CustomerSpaceProperties spaceProperties = new CustomerSpaceProperties();
        spaceProperties.description = tenantProperties.description;
        spaceProperties.displayName = tenantProperties.displayName;

        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(spaceProperties, JsonUtils.serialize(defaultValueMap));

        // SpaceConfiguration
        SpaceConfiguration spaceConfiguration = tenantService.getDefaultSpaceConfig();
        spaceConfiguration.setProducts(Arrays.asList(LatticeProduct.LPA3, LatticeProduct.CG, LatticeProduct.DCP));

        Set<String> services = serviceService.getRegisteredServices();

        List<SerializableDocumentDirectory> configDirs = new ArrayList<>();

        for (String component : services) {
            SerializableDocumentDirectory componentConfig = serviceService.getDefaultServiceConfig(component);
            if(component.equalsIgnoreCase(PLSComponent.componentName)){
                for (SerializableDocumentDirectory.Node node : componentConfig.getNodes()) {
                    if (node.getNode().contains("ExternalAdminEmails")) {
                        StringBuilder mails = new StringBuilder("[");
                        for(User user : vboRequest.getProduct().getUsers()) {
                            mails.append("\"").append(user.getEmailAddress()).append("\";");
                        }
                        mails.append("]");
                        node.setData(mails.toString());
                    }
                }
            }
            componentConfig.setRootPath("/" + component);
            configDirs.add(componentConfig);
        }

        TenantRegistration registration = new TenantRegistration();
        registration.setContractInfo(new ContractInfo(new ContractProperties()));
        registration.setSpaceConfig(spaceConfiguration);
        registration.setSpaceInfo(spaceInfo);
        registration.setTenantInfo(tenantInfo);
        registration.setConfigDirectories(configDirs);
        return tenantService.createTenant("", tenantName.trim(), registration, userName);
    }

    private String getUserName(HttpServletRequest request) {
        String ticket = request.getHeader(Constants.AUTHORIZATION);
        String userName = "_defaultUser";
        if (!StringUtils.isEmpty(ticket)) {
            String decrypted = CipherUtils.decrypt(ticket);
            String[] tokens = decrypted.split("\\|");
            userName = tokens[0];
        }
        return userName;
    }
}
