package com.latticeengines.saml;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.PostConstruct;

import org.opensaml.saml2.metadata.provider.MetadataProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.saml.metadata.MetadataManager;
import org.springframework.stereotype.Component;

@Component
public class MetadataSynchronizer {
    @Autowired
    private MetadataManager metadataManager;

    private Timer timer;

    public MetadataSynchronizer() {
    }

    @PostConstruct
    private void postConstruct() {
        this.timer = new Timer("Metadata-refresh", true);
        this.timer.schedule(new MetadataSynchronizer.RefreshTask(), 5000, 10000);
    }

    public List<MetadataProvider> retrieveMetadata() {
        return new ArrayList<MetadataProvider>();
    }

    private class RefreshTask extends TimerTask {
        private RefreshTask() {
        }

        public void run() {
//            try {
//                // metadataManager.setProviders(retrieveMetadata());
//            } catch (MetadataProviderException e) {
//                throw new RuntimeException(e);
//            }
        }
    }

}
