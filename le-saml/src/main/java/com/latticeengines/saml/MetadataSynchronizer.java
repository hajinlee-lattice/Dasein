package com.latticeengines.saml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.opensaml.saml2.metadata.Endpoint;
import org.opensaml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml2.metadata.SPSSODescriptor;
import org.opensaml.saml2.metadata.provider.MetadataProvider;
import org.opensaml.xml.parse.ParserPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.saml.metadata.ExtendedMetadata;
import org.springframework.security.saml.metadata.ExtendedMetadataDelegate;
import org.springframework.security.saml.metadata.MetadataManager;
import org.springframework.security.saml.metadata.MetadataMemoryProvider;
import org.springframework.security.saml.websso.WebSSOProfileConsumer;
import org.springframework.security.saml.websso.WebSSOProfileConsumerImpl;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.entitymgr.IdentityProviderEntityMgr;
import com.latticeengines.saml.util.SAMLUtils;

public class MetadataSynchronizer {
    private static final Logger log = LoggerFactory.getLogger(MetadataSynchronizer.class);

    private static final String SP_METADATA_TEMPLATE = "/metadata/applatticeenginescom_sp.xml";

    @Inject
    private MetadataManager metadataManager;

    @Inject
    private IdentityProviderEntityMgr identityProviderEntityMgr;

    @Inject
    private ExtendedMetadata baseServiceProviderMetadata;

    @Inject
    private ExtendedMetadata baseIdentityProviderMetadata;

    @Inject
    private ParserPool parserPool;

    @Inject
    private List<WebSSOProfileConsumer> webSSOProfileConsumerBeans;

    @Value("${security.app.public.url:https://localhost:3000}")
    private String publicBaseAddress;

    @Value("${saml.metadata.refresh.frequency:5000}")
    private Integer refreshFrequency;

    @Value("${saml.timeout.authNInstant.hours:12}")
    private Integer authNInstantTimeoutHours;

    private Timer timer;

    @PostConstruct
    private void postConstruct() {
        this.timer = new Timer("Metadata-refresh", true);
        this.timer.schedule(new MetadataSynchronizer.RefreshTask(), 0, refreshFrequency);
        if (CollectionUtils.isNotEmpty(webSSOProfileConsumerBeans)) {
            webSSOProfileConsumerBeans.stream() //
                    .filter(consumer -> consumer instanceof WebSSOProfileConsumerImpl) //
                    .forEach(consumer -> {
                        log.info(String.format("Setting authNInstantTimeout limit to %d hours in bean of classtype %s.",
                                authNInstantTimeoutHours, consumer.getClass().getName()));
                        ((WebSSOProfileConsumerImpl) consumer)
                                .setMaxAuthenticationAge(TimeUnit.HOURS.toSeconds(authNInstantTimeoutHours));
                    });
        }
    }

    @Transactional
    private class RefreshTask extends TimerTask {
        private RefreshTask() {
        }

        public void run() {
            try {
                List<Tenant> tenants = getTenants();
                List<MetadataProvider> providers = new ArrayList<>();

                for (Tenant tenant : tenants) {
                    providers.add(tenant.serviceProvider);
                    providers.addAll(tenant.identityProviders);
                }
                metadataManager.setProviders(providers);
                metadataManager.refreshMetadata();
            } catch (Exception e) {
                log.error("Exception encountered in MetadataSynchronizer refresh task", e);
            }
        }
    }

    public List<Tenant> getTenants() {
        List<IdentityProvider> identityProviders = identityProviderEntityMgr.findAll();
        Map<String, List<IdentityProvider>> grouped = new HashMap<>();
        for (IdentityProvider identityProvider : identityProviders) {
            String tenantId = identityProvider.getGlobalAuthTenant().getId();
            List<IdentityProvider> list;
            if (!grouped.containsKey(tenantId)) {
                grouped.put(tenantId, new ArrayList<IdentityProvider>());
            }
            list = grouped.get(tenantId);
            list.add(identityProvider);
        }

        List<Tenant> tenants = new ArrayList<>();
        for (String tenantId : grouped.keySet()) {
            try {
                log.debug(String.format("Refreshing metadata for tenant %s", tenantId));
                tenants.add(constructTenant(tenantId, grouped.get(tenantId)));
            } catch (Exception e) {
                log.error(
                        String.format("Exception encountered attempting to construct provider for tenant %s", tenantId),
                        e);
            }
        }

        return tenants;
    }

    private Tenant constructTenant(String tenantId, List<IdentityProvider> identityProviders) {
        Tenant tenant = new Tenant();
        for (IdentityProvider identityProvider : identityProviders) {
            IdentityProviderMetadataAdaptor adaptor = new IdentityProviderMetadataAdaptor(parserPool, identityProvider,
                    baseIdentityProviderMetadata);
            tenant.identityProviders.add(adaptor);
        }

        tenant.serviceProvider = constructServiceProvider(tenantId);
        return tenant;
    }

    private MetadataProvider constructServiceProvider(String tenantId) {

        try (InputStream metadataInput = getClass().getResourceAsStream(SP_METADATA_TEMPLATE)) {
            EntityDescriptor entityDescriptor = (EntityDescriptor) SAMLUtils.deserialize(parserPool, metadataInput);
            ExtendedMetadata extendedMetadata = baseServiceProviderMetadata.clone();
            extendedMetadata.setAlias(tenantId);

            String entityId = entityDescriptor.getEntityID();
            entityId = replacePlaceholders(tenantId, entityId);

            String id = entityDescriptor.getID();
            id = replacePlaceholders(tenantId, id);

            entityDescriptor.setEntityID(entityId);
            entityDescriptor.setID(id);

            MetadataMemoryProvider memoryProvider = new MetadataMemoryProvider(entityDescriptor);
            memoryProvider.initialize();

            MetadataProvider serviceProvider = new ExtendedMetadataDelegate(memoryProvider, extendedMetadata);
            SPSSODescriptor descriptor = (SPSSODescriptor) serviceProvider.getRole(entityId,
                    SPSSODescriptor.DEFAULT_ELEMENT_NAME, "urn:oasis:names:tc:SAML:2.0:protocol");
            descriptor.setWantAssertionsSigned(true);

            for (Endpoint endpoint : descriptor.getEndpoints()) {
                String location = endpoint.getLocation();
                location = replacePlaceholders(tenantId, location);
                endpoint.setLocation(location);
            }

            String idpDiscoveryResponseURL = extendedMetadata.getIdpDiscoveryResponseURL();
            idpDiscoveryResponseURL = replacePlaceholders(tenantId, idpDiscoveryResponseURL);
            extendedMetadata.setIdpDiscoveryResponseURL(idpDiscoveryResponseURL);

            String idpDiscoveryURL = extendedMetadata.getIdpDiscoveryURL();
            idpDiscoveryURL = replacePlaceholders(tenantId, idpDiscoveryURL);
            extendedMetadata.setIdpDiscoveryResponseURL(idpDiscoveryResponseURL);
            extendedMetadata.setIdpDiscoveryURL(idpDiscoveryURL);

            return serviceProvider;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String replacePlaceholders(String tenantId, String uri) {
        uri = uri.replace("___TENANT_ID___", tenantId);
        return uri.replace("___BASE_ADDRESS___", publicBaseAddress + "/pls");
    }

    private class Tenant {
        public List<MetadataProvider> identityProviders = new ArrayList<>();
        public MetadataProvider serviceProvider;
    }
}
