package com.latticeengines.saml;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.PostConstruct;

import org.opensaml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml2.metadata.provider.MetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.xml.XMLObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.saml.metadata.MetadataManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.entitymgr.IdentityProviderEntityMgr;

@Component
public class MetadataSynchronizer {
    @Autowired
    private MetadataManager metadataManager;

    @Autowired
    private IdentityProviderEntityMgr identityProviderEntityMgr;

    private Timer timer;

    public MetadataSynchronizer() {
    }

    @PostConstruct
    private void postConstruct() {
        this.timer = new Timer("Metadata-refresh", true);
        this.timer.schedule(new MetadataSynchronizer.RefreshTask(), 5000, 10000);
    }

    @Transactional
    private class RefreshTask extends TimerTask {
        private RefreshTask() {
        }

        public void run() {
            try {
                List<MetadataProvider> providers = getServiceProviders();
                providers.addAll(getIdentityProviders());
                metadataManager.setProviders(providers);
            } catch (MetadataProviderException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public List<MetadataProvider> getIdentityProviders() {
        List<IdentityProvider> idps = identityProviderEntityMgr.findAll();
        return new ArrayList<>();
    }

    private List<MetadataProvider> getServiceProviders() {
        return new ArrayList<>();
    }

    private String getEntityId(MetadataProvider provider) throws MetadataProviderException {
        XMLObject object = provider.getMetadata();
        if (object instanceof EntityDescriptor) {
            return ((EntityDescriptor) object).getEntityID();
        }
        throw new RuntimeException("Multiple entity ids are not supported");
    }

}
