package com.latticeengines.saml.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.opensaml.common.xml.SAMLConstants;
import org.opensaml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.saml2.metadata.SingleSignOnService;
import org.opensaml.xml.parse.ParserPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.domain.exposed.saml.IdpMetadataValidationResponse;
import com.latticeengines.domain.exposed.saml.SamlConfigMetadata;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.saml.entitymgr.IdentityProviderEntityMgr;
import com.latticeengines.saml.service.IdentityProviderService;
import com.latticeengines.saml.util.SAMLUtils;
import com.latticeengines.security.exposed.TicketAuthenticationToken;

@Component("IdentityProviderService")
public class IdentityProviderServiceImpl implements IdentityProviderService {
    private static final Logger log = LoggerFactory.getLogger(IdentityProviderServiceImpl.class);

    @Inject
    private IdentityProviderEntityMgr identityProviderEntityMgr;

    @Inject
    private GlobalAuthTenantEntityMgr globalAuthTenantEntityMgr;

    @Inject
    private ParserPool parserPool;

    @Override
    public void create(IdentityProvider identityProvider) {
        Tenant tenant = getTenant();
        GlobalAuthTenant gatenant = globalAuthTenantEntityMgr.findByTenantId(tenant.getId());
        identityProvider.setGlobalAuthTenant(gatenant);
        identityProviderEntityMgr.create(identityProvider);
    }

    @Override
    public void delete(String entityId) {
        Tenant tenant = getTenant();
        GlobalAuthTenant gaTenant = globalAuthTenantEntityMgr.findByTenantId(tenant.getId());
        IdentityProvider identityProvider = identityProviderEntityMgr.findByGATenantAndEntityId(gaTenant, entityId);
        if (identityProvider == null) {
            throw new AccessDeniedException("Unauthorized");
        }

        identityProviderEntityMgr.delete(identityProvider);
    }

    @Override
    public IdentityProvider find(String entityId) {
        Tenant tenant = getTenant();
        GlobalAuthTenant gaTenant = globalAuthTenantEntityMgr.findByTenantId(tenant.getId());
        IdentityProvider identityProvider = identityProviderEntityMgr.findByGATenantAndEntityId(gaTenant, entityId);
        if (identityProvider == null) {
            return null;
        }
        if (!identityProvider.getGlobalAuthTenant().getId().equals(tenant.getId())) {
            throw new AccessDeniedException("Unauthorized");
        }

        return identityProvider;
    }

    @Override
    public List<IdentityProvider> findAll() {
        Tenant tenant = getTenant();
        GlobalAuthTenant gatenant = globalAuthTenantEntityMgr.findByTenantId(tenant.getId());
        return identityProviderEntityMgr.findByTenantId(gatenant.getId());
    }

    private Tenant getTenant() {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            TicketAuthenticationToken auth = (TicketAuthenticationToken) SecurityContextHolder.getContext()
                    .getAuthentication();
            Session session = auth.getSession();
            tenant = session.getTenant();

            if (tenant == null) {
                throw new RuntimeException("No tenant supplied in context");
            }
        }
        return tenant;
    }

    @Override
    public IdpMetadataValidationResponse validate(IdentityProvider identityProvider) {
        IdpMetadataValidationResponse resp = new IdpMetadataValidationResponse();
        resp.setValid(true);

        try {
            SamlConfigMetadata samlConfigMetadata = extractMetadataConfig(identityProvider);
            resp.setEntityId(samlConfigMetadata.getEntityId());
            resp.setSingleSignOnService(samlConfigMetadata.getSingleSignOnService());
        } catch (Exception e) {
            resp.setValid(false);
            Exception ex = new LedpException(LedpCode.LEDP_33001, new String[] { e.getMessage() });
            resp.setExceptionMessage(ex.getMessage());
            log.error("Error:",  e);
        }
        return resp;
    }

    private SamlConfigMetadata extractMetadataConfig(IdentityProvider identityProvider) {
        if (identityProvider.getMetadata() == null) {
            throw new LedpException(LedpCode.LEDP_33001, new String[] { "Metadata XML is empty" });
        }

        EntityDescriptor descriptor = (EntityDescriptor) SAMLUtils.deserialize(parserPool,
                identityProvider.getMetadata());

        // Get SingleSignonURL
        IDPSSODescriptor ssoDescriptor = descriptor.getIDPSSODescriptor(SAMLConstants.SAML20P_NS);
        if (ssoDescriptor == null) {
            throw new LedpException(LedpCode.LEDP_33001, new String[] { "Could not find SSO Descriptor" });
        }
        SingleSignOnService ssoPostBinding = ssoDescriptor.getSingleSignOnServices().stream()
                .filter(sso -> sso.getBinding().equals(SAMLConstants.SAML2_POST_BINDING_URI)).findAny()
                .orElseThrow(() -> new LedpException(LedpCode.LEDP_33001,
                        new String[] { "Could not find SSO POST binding" }));
        String ssoBindingUrl = ssoPostBinding.getLocation();

        SamlConfigMetadata samlConfigMetadata = new SamlConfigMetadata();
        samlConfigMetadata.setEntityId(descriptor.getEntityID());
        samlConfigMetadata.setSingleSignOnService(ssoBindingUrl);

        return samlConfigMetadata;
    }

    @Override
    public SamlConfigMetadata getConfigMetadata(Tenant tenant) {
        GlobalAuthTenant gatenant = globalAuthTenantEntityMgr.findByTenantId(tenant.getId());
        if (gatenant == null) {
            throw new LedpException(LedpCode.LEDP_33003, new String[] {tenant.getId()});
        }
        List<IdentityProvider> identityProviders = identityProviderEntityMgr.findByTenantId(gatenant.getId());
        if (CollectionUtils.isEmpty(identityProviders)) {
            throw new LedpException(LedpCode.LEDP_33004, new String[] {tenant.getId()});
        }

        IdentityProvider idp = identityProviders.get(0);
        return extractMetadataConfig(idp);

    }
}
