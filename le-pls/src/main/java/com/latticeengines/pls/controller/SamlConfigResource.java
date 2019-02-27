package com.latticeengines.pls.controller;

import java.util.Random;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.domain.exposed.saml.IdpMetadataValidationResponse;
import com.latticeengines.domain.exposed.saml.ServiceProviderURIInfo;
import com.latticeengines.proxy.exposed.saml.SamlConfigProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "saml-config", description = "REST resource for saml configuration")
@RestController
@RequestMapping("/saml-config")
@PreAuthorize("hasRole('View_PLS_SSO_Config')")
public class SamlConfigResource {

    private static final Logger log = LoggerFactory.getLogger(SamlConfigResource.class);

    private static String assertionConsumerServiceURLTemplate = "%s/pls/saml/login/%s";
    private static String serviceProviderMetadataURLTemplate = "%s/pls/saml/metadata/%s";
    private static String serviceProviderEntityIdTemplate = "%s/pls/saml/%s";

    @Value("${security.app.public.url:https://localhost:3000}")
    private String plsUrl;

    @Autowired
    private SamlConfigProxy samlConfigProxy;

    @RequestMapping(value = "/sp-uri-info", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Service Provider URI Info")
    public ServiceProviderURIInfo getSPUriInfo(HttpServletRequest request) {
        String tenantId = MultiTenantContext.getTenant().getId();
        ServiceProviderURIInfo spUriInfo = new ServiceProviderURIInfo();
        spUriInfo.setAssertionConsumerServiceURL( //
                String.format(assertionConsumerServiceURLTemplate, plsUrl, tenantId));
        // TODO - modify this with random UUID (backed by saving to repos) as
        // part of fix for PLS-6441
        Long randomizedTenantId = (new Random(tenantId.hashCode())).nextLong();
        spUriInfo.setServiceProviderMetadataURL( //
                String.format(serviceProviderMetadataURLTemplate, plsUrl, randomizedTenantId));
        spUriInfo.setServiceProviderEntityId( //
                String.format(serviceProviderEntityIdTemplate, plsUrl, tenantId));
        return spUriInfo;
    }

    @RequestMapping(value = "", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get SSO/Saml configuration")
    public IdentityProvider getConfig() {
        String tenantId = MultiTenantContext.getTenant().getId();
        log.info("Retrieving identity provider config");
        return samlConfigProxy.getConfig(tenantId);
    }

    @RequestMapping(value = "/validate", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Validate SSO/Saml configuration")
    public IdpMetadataValidationResponse validateConfig(@RequestBody IdentityProvider identityProvider) {
        String tenantId = MultiTenantContext.getTenant().getId();
        log.info("Validating identity provider config");
        return samlConfigProxy.validateMetadata(tenantId, identityProvider);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create SSO/Saml configuration")
    public void createConfig(@RequestBody IdentityProvider identityProvider) {
        String tenantId = MultiTenantContext.getTenant().getId();
        log.info("Creating identity provider config");
        samlConfigProxy.saveConfig(tenantId, identityProvider);
    }

    @RequestMapping(value = "/{configId}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete SSO/Saml configuration")
    public void deleteConfig(@PathVariable String configId) {
        String tenantId = MultiTenantContext.getTenant().getId();
        log.info("Deleting identity provider config");
        samlConfigProxy.deleteConfig(tenantId, configId);
    }

}
