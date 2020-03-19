package com.latticeengines.admin.controller.dcp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.service.FeatureFlagService;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.cdl.CDLComponent;
import com.latticeengines.admin.tenant.batonadapter.datacloud.DataCloudComponent;
import com.latticeengines.admin.tenant.batonadapter.dcp.DCPComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.domain.exposed.dcp.vbo.VboResponse;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.service.IDaaSService;
import com.latticeengines.security.service.impl.IDaaSUser;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "vboadmin", description = "REST resource for vbo request")
@RestController
@RequestMapping(value = "/dcp/vboadmin")
@PostAuthorize("hasRole('adminconsole')")
public class VboResource {

    private static final String DCP_PRODUCT = "Data Cloud Portal";
    private static final String DCP_ROLE = "DATA_CLOUD_PORTAL_ACCESS";

    @Inject
    private TenantService tenantService;

    @Inject
    private ServiceService serviceService;

    @Inject
    private FeatureFlagService featureFlagService;

    @Inject
    private IDaaSService iDaaSService;

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Create a DCP tenant from VBO request")
    public VboResponse createTenant(@RequestBody VboRequest vboRequest, HttpServletRequest request) {
        VboResponse vboResponse = new VboResponse();
        try{
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

            List<String> services = Arrays.asList(PLSComponent.componentName, CDLComponent.componentName, DataCloudComponent.componentName, DCPComponent.componentName);

            List<SerializableDocumentDirectory> configDirs = new ArrayList<>();

            for (String component : services) {
                SerializableDocumentDirectory componentConfig = serviceService.getDefaultServiceConfig(component);
                if(component.equalsIgnoreCase(PLSComponent.componentName)){
                    for (SerializableDocumentDirectory.Node node : componentConfig.getNodes()) {
                        if (node.getNode().contains("ExternalAdminEmails")) {
                            StringBuilder mails = new StringBuilder("[");
                            for(VboRequest.User user : vboRequest.getProduct().getUsers()) {
                                mails.append("\"").append(user.getEmailAddress()).append("\",");
                                // to add create IDaaS user after interface ready
                                createIDaaSUser(user, vboRequest.getSubscriber().getLanguage());
                            }
                            mails.deleteCharAt(mails.lastIndexOf(","));
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


            boolean result = tenantService.createTenant(tenantName.trim(), tenantName.trim(), registration, userName);
            if (result) {
                vboResponse.setStatus("success");
                vboResponse.setMessage("tenant " + tenantName.trim() + "created successfully via Vbo request");
            } else {
                vboResponse.setStatus("failed");
                vboResponse.setMessage("tenant " + tenantName.trim() + "created failed via Vbo request");
            }
        } catch(Exception e){
            vboResponse.setStatus("failed");
            vboResponse.setMessage("tenant created failed via Vbo request," + e.getMessage());
        }
        return vboResponse;
    }

    private void createIDaaSUser(VboRequest.User user, String language) {
        IDaaSUser iDaasuser = new IDaaSUser();
        iDaasuser.setFirstName(user.getName().getFirstName());
        iDaasuser.setEmailAddress(user.getEmailAddress());
        iDaasuser.setLastName(user.getName().getLastName());
        iDaasuser.setUserName(user.getUserId());
        iDaasuser.setPhoneNumber(user.getTelephoneNumber());
        iDaasuser.setLanguage(language);
        iDaaSService.createIDaaSUser(iDaasuser);
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
