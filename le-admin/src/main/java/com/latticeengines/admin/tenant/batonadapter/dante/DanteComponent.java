package com.latticeengines.admin.tenant.batonadapter.dante;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.proxy.exposed.RestApiClient;

@Component
public class DanteComponent extends LatticeComponent {
    public static final String componentName = "Dante";

    private static final Logger LOGGER = LoggerFactory.getLogger(DanteComponent.class);

    @Value("${admin.dante.dryrun}")
    private boolean dryrun;

    @Value("${admin.dante.hosts:}")
    private String danteHosts;

    @Inject
    private ApplicationContext applicationContext;

    private LatticeComponentInstaller installer = new DanteInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new DanteUpgrader();
    private CustomerSpaceServiceDestroyer destroyer = new DanteDestroyer();

    private Map<String, RestApiClient> danteClients;

    @PostConstruct
    public void postConstruct() {
        danteClients = new HashMap<>();
        String[] hosts = danteHosts.split(",");
        for (String host: hosts) {
            if (StringUtils.isNotEmpty(host) && host.contains(":\\\\")) {
                RestApiClient restApiClient = RestApiClient.newInternalClient(applicationContext, host);
                danteClients.put(host, restApiClient);
            }
        }
    }

    @Override
    public Set<LatticeProduct> getAssociatedProducts() {
        return new HashSet<>(Collections.singleton(LatticeProduct.LPA));
    }

    @Override
    public String getName() {
        return componentName;
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() {
        installer.setDryrun(false);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public CustomerSpaceServiceDestroyer getDestroyer() {
        return destroyer;
    }

    @Override
    public String getVersionString() {
        return null;
    }

    @Override
    public boolean doRegistration() {
        String defaultJson = "dante_default.json";
        String metadataJson = "dante_metadata.json";
        uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
        // register dummy installer if is in dryrun mode
        return dryrun;
    }

    public void wakeUpAppPool() {
        for (Map.Entry<String, RestApiClient> entry: danteClients.entrySet()) {
            try {
                entry.getValue().get("/") ;
                LOGGER.info("Wake up BIS server by sending a GET to " + entry.getKey());
            } catch (Exception e) {
                LOGGER.error("Waking up BIS server at " + entry.getKey() + " failed", e);
            }
        }
    }

}
