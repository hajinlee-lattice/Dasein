package com.latticeengines.saml.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.domain.exposed.saml.IdpMetadataValidationResponse;
import com.latticeengines.domain.exposed.saml.SamlConfigMetadata;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.saml.service.IdentityProviderService;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.service.TenantService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "identityproviders", description = "REST resource for managing SAML identity providers")
@RestController
@RequestMapping("/management/identityproviders")
public class IdentityProviderConfigResource {
    private static final Logger log = LoggerFactory.getLogger(IdentityProviderConfigResource.class);
    private static final String TENANT_ID_PATH = "{tenantId:\\w+\\.\\w+\\.\\w+}";

    @Inject
    private TenantService tenantService;

    @Inject
    private IdentityProviderService identityProviderService;

    @GetMapping(TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Retrieve all identity providers")
    public List<IdentityProvider> findAll(@PathVariable("tenantId") String tenantId) {
        manufactureSecurityContextForInternalAccess(tenantId);
        log.info("Retrieving all identity providers");
        return identityProviderService.findAll();
    }

    @GetMapping(TENANT_ID_PATH +"/config-metadata")
    @ResponseBody
    @ApiOperation(value = "Retrieve all identity providers")
    public SamlConfigMetadata getSamlConfigMetadata(@PathVariable("tenantId") String tenantId) {
        Tenant tenant = manufactureSecurityContextForInternalAccess(tenantId);
        log.info("Retrieving Config Metadata");
        return identityProviderService.getConfigMetadata(tenant);
    }

    @PostMapping(TENANT_ID_PATH + "/validate")
    @ResponseBody
    @ApiOperation(value = "Validate an identity provider config")
    public IdpMetadataValidationResponse validate(@PathVariable("tenantId") String tenantId,
            @RequestBody IdentityProvider identityProvider) {
        manufactureSecurityContextForInternalAccess(tenantId);
        log.info(String.format("Validating identity provider with entityId %s", identityProvider.getEntityId()));
        return identityProviderService.validate(identityProvider);
    }

    @PostMapping(TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Create an identity provider")
    public void create(@PathVariable("tenantId") String tenantId, @RequestBody IdentityProvider identityProvider) {
        manufactureSecurityContextForInternalAccess(tenantId);
        log.info(String.format("Creating identity provider with entityId %s", identityProvider.getEntityId()));
        identityProviderService.create(identityProvider);
    }

    @DeleteMapping(TENANT_ID_PATH + "/{configId}")
    @ResponseBody
    @ApiOperation(value = "Delete a identity provider with the specified entityId")
    public void delete(@PathVariable("tenantId") String tenantId, @PathVariable String configId) {
        manufactureSecurityContextForInternalAccess(tenantId);
        String entityId = new String(Base64Utils.decodeBase64(configId));
        log.info(String.format("Deleting identity provider with entityIdBase64 %s", entityId));
        identityProviderService.delete(entityId);
    }

    private Tenant manufactureSecurityContextForInternalAccess(String tenantId) {
        log.info("Manufacturing security context for " + tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_18074, new String[] { tenantId });
        }
        manufactureSecurityContextForInternalAccess(tenant);
        return tenant;
    }

    private void manufactureSecurityContextForInternalAccess(Tenant tenant) {
        TicketAuthenticationToken auth = new TicketAuthenticationToken(null, "x.y");
        Session session = new Session();
        session.setTenant(tenant);
        auth.setSession(session);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }
}
