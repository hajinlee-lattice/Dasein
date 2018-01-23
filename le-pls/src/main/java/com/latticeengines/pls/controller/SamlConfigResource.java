package com.latticeengines.pls.controller;

import java.util.Random;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.saml.ServiceProviderURIInfo;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "saml-config", description = "REST resource for saml configuration")
@RestController
@RequestMapping("/saml-config")
@PreAuthorize("hasRole('View_PLS_SSO_Config')")
public class SamlConfigResource {

    private static String assertionConsumerServiceURLTemplate = "%s/pls/saml/login/%s";
    private static String serviceProviderMetadataURLTemplate = "%s/pls/saml/metadata/%s";
    private static String serviceProviderEntityIdTemplate = "%s/saml/%s";

    @Value("${security.app.public.url:https://localhost:3000}")
    private String plsUrl;

    @RequestMapping(value = "/sp-uri-info", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Service Provider URI Info")
    public ServiceProviderURIInfo getPlays(HttpServletRequest request) {
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

}
